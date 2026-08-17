using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using Sluice.Fabric;
using Sluice.Fusion;

namespace Sluice.Supergraph;

/// <summary>
/// A participant in a <b>federated reactive graph</b>. Each process hosts one <see cref="GraphPeer"/>; together
/// they form one logical graph whose vertices are spread across shared memory and the network. A participant
/// <see cref="Define{T}">owns</see> some vertices (its source of truth) and <see cref="Observe{T}">observes</see>
/// or <see cref="Computed{T}">computes</see> over others, wherever they live.
///
/// <para>
/// The model is Fusion's "signal stale, recompute on demand," generalized over <see cref="Sluice.Fabric"/> so it
/// spans hosts:
/// </para>
/// <list type="bullet">
/// <item><b>Reads of a vertex you own are a direct local read</b> — no round-trip, no copy out of your own store.
/// This is the zero-cost fast path the fabric was built to preserve.</item>
/// <item><b>Reads of a vertex another participant owns</b> route a correlated fetch to that owner over the
/// channel; a co-located owner answers through shared memory, a remote owner over the network — the caller code
/// is identical.</item>
/// <item><b>A change anywhere broadcasts a tiny invalidation</b> (key + version, never the value) across the
/// whole federation in one call. Mirrors mark their cached copy stale and refetch lazily on next read.</item>
/// <item><b>A <see cref="Computed{T}">computed</see> vertex</b> re-derives from its dependencies — which may be
/// remote — and, when it changes, publishes its own invalidation. So a change propagates transitively across
/// participants: the whole supergraph reacts as one.</item>
/// </list>
/// </summary>
public sealed class GraphPeer : IDisposable
{
    // ---- wire protocol over an IChannel -------------------------------------------------------------
    // Inval  (broadcast)  : [version:8][key utf8]                      — owner → everyone, "this vertex changed"
    // GetReq (addressed)  : [corr:8][key utf8]                        — mirror → owner, "send me this vertex"
    // GetRep (addressed)  : [corr:8][version:8][value]               — owner → mirror, the value (version<0 = miss)
    internal const int KindInval = 1;
    internal const int KindGetReq = 2;
    internal const int KindGetRep = 3;

    private readonly ITransport _transport;
    private readonly IChannel _channel;
    private readonly IDisposable _subscription;
    private readonly ParticipantId _self;
    private readonly TimeSpan _fetchTimeout;
    private readonly bool _ownsTransport;

    // Vertices this participant owns (source of truth).
    private readonly ConcurrentDictionary<string, Entry> _owned = new(StringComparer.Ordinal);
    // Cached snapshots of vertices owned by others.
    private readonly ConcurrentDictionary<VertexId, Cached> _cache = new();
    // In-flight fetches awaiting a GetRep, keyed by correlation id.
    private readonly ConcurrentDictionary<long, TaskCompletionSource<(long Version, byte[] Value)>> _pending = new();
    // Per-vertex invalidation observers (an ObservedVertex / ComputedVertex registers here).
    private readonly ConcurrentDictionary<VertexId, ObserverSet> _observers = new();

    private long _versionClock;
    private long _corrClock;

    // Set on the calling thread when a fetch gets NO reply (lost/timed-out), as opposed to a reply that says
    // "no such vertex" (a real miss). A reactive loop resets it before a read and checks it after, so it can tell
    // "the owner answered, I'm settled" from "my request was lost, retry" — the property that lets the federation
    // self-heal a dropped message instead of wedging on it. [ThreadStatic] is safe because every read a given
    // observer/computed performs runs on that loop's own thread.
    [ThreadStatic] private static bool _fetchUnanswered;
    internal static void BeginRead() => _fetchUnanswered = false;
    internal static bool LastReadWasLost => _fetchUnanswered;

    public ParticipantId Self => _self;
    public string GraphName { get; }

    /// <summary>The participants currently believed present on the graph (by the channel's liveness view).</summary>
    public IReadOnlyCollection<ParticipantId> Participants => _channel.Participants;

    /// <summary>
    /// Join the graph over a transport. Pass a <see cref="FederatedTransport"/> to span shared memory and the
    /// network; pass a bare <see cref="ShmTransport"/> for a same-host-only graph. The supergraph does not take
    /// ownership of an externally-supplied transport unless <paramref name="ownsTransport"/> is set.
    /// </summary>
    public GraphPeer(ITransport transport, string graphName = "supergraph", TimeSpan? fetchTimeout = null,
        bool ownsTransport = false)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        GraphName = graphName;
        _channel = transport.Open(graphName);
        _self = _channel.Self;
        _fetchTimeout = fetchTimeout ?? TimeSpan.FromSeconds(2);   // a lost fetch self-heals via retry, so detect it promptly
        _ownsTransport = ownsTransport;
        _subscription = _channel.Subscribe(OnMessage);
    }

    // ---- public typed surface -----------------------------------------------------------------------

    /// <summary>Define a vertex this participant owns. The returned handle's <c>Set</c> updates the value and
    /// invalidates every mirror across the federation.</summary>
    public SourceVertex<T> Define<T>(string key, SluiceCodec<T> codec) => new(this, key, codec);

    /// <summary>A self-refreshing typed view of any vertex (local or remote): it reads the current value, then
    /// refetches and raises <c>Updated</c> every time the owner changes it.</summary>
    public ObservedVertex<T> Observe<T>(VertexId id, SluiceCodec<T> codec) => new(this, id, codec);

    /// <summary>Define a computed vertex this participant owns, derived from <paramref name="dependencies"/> that
    /// may live on other participants. It recomputes (re-reading its deps, routing each as needed) whenever any
    /// dependency is invalidated, and publishes its own invalidation so downstream vertices react in turn.</summary>
    public ComputedVertex<T> Computed<T>(string key, SluiceCodec<T> codec, Func<T> compute,
        params VertexId[] dependencies) => new(this, key, codec, compute, dependencies);

    /// <summary>Read the current snapshot of a vertex: a direct local read if you own it, otherwise a fetch from
    /// its owner (served from cache while consistent). The result carries the value, version, and the
    /// <see cref="Reach"/> it came from.</summary>
    public Computed<T> Get<T>(VertexId id, SluiceCodec<T> codec, CancellationToken ct = default)
    {
        var (version, value, reach, exists) = GetRaw(id, ct);
        T typed = exists ? codec.Deserialize(value) : default!;
        return new Computed<T>(id, typed, version, reach, value);
    }

    // ---- core read/write ----------------------------------------------------------------------------

    internal ParticipantId SelfId => _self;

    internal void SetOwned(string key, byte[] value)
    {
        long v = Interlocked.Increment(ref _versionClock);
        _owned[key] = new Entry(v, value);
        // Broadcast a tiny invalidation to the whole federation. (We are filtered out of our own broadcast, so
        // local dependents on this vertex are notified directly below.)
        _channel.Broadcast(KindInval, EncodeInval(key, v));
        NotifyObservers(new VertexId(_self, key), v);
    }

    internal (long Version, byte[] Value, Reach Reach, bool Exists) GetRaw(VertexId id, CancellationToken ct)
    {
        // Fast path: a vertex we own is a direct read of our store — no round-trip, no copy out.
        if (id.Owner == _self)
        {
            return _owned.TryGetValue(id.Key, out var mine)
                ? (mine.Version, mine.Value, Reach.Local, true)
                : (-1L, Array.Empty<byte>(), Reach.Local, false);
        }

        // Serve from cache while it is consistent.
        if (_cache.TryGetValue(id, out var cached) && !cached.Stale)
            return (cached.Version, cached.Value, ReachOf(id), cached.Version >= 0);

        // Otherwise fetch from the owner and reconcile the cache.
        var (ver, val, answered) = Fetch(id, ct);
        var slot = _cache.GetOrAdd(id, _ => new Cached());
        lock (slot.Gate)
        {
            if (answered)
            {
                // The owner replied (a value, or "no such vertex" at ver == -1). Don't let an equal-or-older
                // version clobber a fresher one a concurrent path cached.
                if (ver >= slot.Version)
                {
                    slot.Version = ver;
                    slot.Value = val;
                }
                // Consistent only if we are at least as new as every invalidation we have seen. If a newer
                // invalidation already arrived (the value changed again, or raced ahead of this fetch), this
                // reply is stale on arrival — keep it stale and retry rather than treating it as settled.
                if (slot.Version >= slot.InvalVersion)
                {
                    slot.Stale = false;
                }
                else
                {
                    slot.Stale = true;
                    _fetchUnanswered = true;
                }
            }
            else
            {
                // The request was lost / timed out. Stay stale so the next read retries, and tell the caller's
                // loop (via the thread-static) that it must retry rather than treat this as settled.
                _fetchUnanswered = true;
            }
            return (slot.Version, slot.Value, ReachOf(id), slot.Version >= 0);
        }
    }

    private (long Version, byte[] Value, bool Answered) Fetch(VertexId id, CancellationToken ct)
    {
        long corr = Interlocked.Increment(ref _corrClock);
        var tcs = new TaskCompletionSource<(long, byte[])>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[corr] = tcs;
        try
        {
            _channel.Send(id.Owner, KindGetReq, EncodeGetReq(corr, id.Key), ct);
            // Blocks the CALLER's thread (never the channel pump — GetRep is dispatched by the pump while we wait).
            if (tcs.Task.Wait((int)_fetchTimeout.TotalMilliseconds, ct))
            {
                var (v, val) = tcs.Task.Result;
                return (v, val, true);              // owner answered (v == -1 means a real "no such vertex")
            }
            return (-1, Array.Empty<byte>(), false);   // no reply → lost/unreachable; caller retries
        }
        catch (OperationCanceledException)
        {
            return (-1, Array.Empty<byte>(), false);
        }
        finally
        {
            _pending.TryRemove(corr, out _);
        }
    }

    private Reach ReachOf(VertexId id) => _transport.Owns(id.Owner) && ReachIsLocal(id) ? Reach.Local : Reach.Remote;

    private bool ReachIsLocal(VertexId id)
    {
        // A FederatedTransport can answer which member carries an id; otherwise fall back to the transport's reach.
        if (_transport is FederatedTransport fed) return fed.RouteTo(id.Owner).Reach == Reach.Local;
        return _transport.Reach == Reach.Local;
    }

    // ---- the channel pump ---------------------------------------------------------------------------

    private void OnMessage(in Inbound m)
    {
        switch (m.Kind)
        {
            case KindInval:
            {
                var (key, version) = DecodeInval(m.Payload);
                var vid = new VertexId(m.From, key);
                // Record the invalidation even if we have no value yet — but only for vertices we actually track
                // (observed or cached), so a big graph's unrelated invalidations don't accrete slots here.
                if (_cache.TryGetValue(vid, out var slot) || _observers.ContainsKey(vid))
                {
                    slot ??= _cache.GetOrAdd(vid, _ => new Cached());
                    lock (slot.Gate)
                    {
                        if (version > slot.InvalVersion) slot.InvalVersion = version;
                        if (slot.Version < version) slot.Stale = true;
                    }
                }
                NotifyObservers(vid, version);   // wake observers/dependents; they refetch on THEIR own thread
                break;
            }
            case KindGetReq:
            {
                var (corr, key) = DecodeGetReq(m.Payload);
                // Reply with our owned value, or an explicit miss — never leave the requester to time out if we can help it.
                if (_owned.TryGetValue(key, out var e))
                    _channel.Send(m.From, KindGetRep, EncodeGetRep(corr, e.Version, e.Value));
                else
                    _channel.Send(m.From, KindGetRep, EncodeGetRep(corr, -1, Array.Empty<byte>()));
                break;
            }
            case KindGetRep:
            {
                var (corr, version, value) = DecodeGetRep(m.Payload);   // copies the value out of the span
                if (_pending.TryGetValue(corr, out var tcs)) tcs.TrySetResult((version, value));
                break;
            }
        }
    }

    // ---- observer registry --------------------------------------------------------------------------

    internal IDisposable RegisterObserver(VertexId id, Action<long> onInvalidated)
    {
        var set = _observers.GetOrAdd(id, _ => new ObserverSet());
        return set.Add(onInvalidated);
    }

    private void NotifyObservers(VertexId id, long version)
    {
        if (_observers.TryGetValue(id, out var set)) set.Fire(version);
    }

    // ---- wire encode/decode (all decoders copy out of the span — it is only valid during the call) ---

    private static byte[] EncodeInval(string key, long version)
    {
        int keyLen = Encoding.UTF8.GetByteCount(key);
        var buf = new byte[8 + keyLen];
        BinaryPrimitives.WriteInt64LittleEndian(buf, version);
        Encoding.UTF8.GetBytes(key, buf.AsSpan(8));
        return buf;
    }

    private static (string Key, long Version) DecodeInval(ReadOnlySpan<byte> p)
        => (Encoding.UTF8.GetString(p[8..]), BinaryPrimitives.ReadInt64LittleEndian(p));

    private static byte[] EncodeGetReq(long corr, string key)
    {
        int keyLen = Encoding.UTF8.GetByteCount(key);
        var buf = new byte[8 + keyLen];
        BinaryPrimitives.WriteInt64LittleEndian(buf, corr);
        Encoding.UTF8.GetBytes(key, buf.AsSpan(8));
        return buf;
    }

    private static (long Corr, string Key) DecodeGetReq(ReadOnlySpan<byte> p)
        => (BinaryPrimitives.ReadInt64LittleEndian(p), Encoding.UTF8.GetString(p[8..]));

    private static byte[] EncodeGetRep(long corr, long version, byte[] value)
    {
        var buf = new byte[16 + value.Length];
        BinaryPrimitives.WriteInt64LittleEndian(buf, corr);
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(8), version);
        value.CopyTo(buf.AsSpan(16));
        return buf;
    }

    private static (long Corr, long Version, byte[] Value) DecodeGetRep(ReadOnlySpan<byte> p)
        => (BinaryPrimitives.ReadInt64LittleEndian(p),
            BinaryPrimitives.ReadInt64LittleEndian(p[8..]),
            p[16..].ToArray());

    public void Dispose()
    {
        _subscription.Dispose();
        _channel.Dispose();
        foreach (var tcs in _pending.Values) tcs.TrySetResult((-1, Array.Empty<byte>()));
        if (_ownsTransport) _transport.Dispose();
    }

    // ---- internal state types -----------------------------------------------------------------------

    private readonly record struct Entry(long Version, byte[] Value);

    private sealed class Cached
    {
        public readonly object Gate = new();
        public long Version = -1;
        public byte[] Value = Array.Empty<byte>();
        public volatile bool Stale;
        // The highest version we have ever seen invalidated for this vertex — tracked even before we hold a value,
        // so an invalidation that races ahead of our first fetch is not forgotten. A fetched value older than this
        // is known-stale on arrival and must be retried, which closes the "invalidation before first cache" race.
        public long InvalVersion = -1;
    }

    private sealed class ObserverSet
    {
        private readonly List<Action<long>> _list = new();
        private readonly object _gate = new();

        public IDisposable Add(Action<long> cb)
        {
            lock (_gate) _list.Add(cb);
            return new Remove(this, cb);
        }

        public void Fire(long version)
        {
            Action<long>[] snapshot;
            lock (_gate) snapshot = _list.ToArray();
            foreach (var cb in snapshot) cb(version);
        }

        private sealed class Remove(ObserverSet owner, Action<long> cb) : IDisposable
        {
            public void Dispose() { lock (owner._gate) owner._list.Remove(cb); }
        }
    }
}
