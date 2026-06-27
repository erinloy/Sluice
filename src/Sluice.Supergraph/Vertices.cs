using Sluice.Fabric;
using Sluice.Fusion;

namespace Sluice.Supergraph;

/// <summary>
/// A handle to a vertex this participant owns. <see cref="Set"/> updates the source-of-truth value and
/// invalidates every mirror of it across the federation. The vertex is addressable everywhere as <see cref="Id"/>.
/// </summary>
public sealed class SourceVertex<T>
{
    private readonly GraphPeer _graph;
    private readonly SluiceCodec<T> _codec;

    public VertexId Id { get; }
    public string Key { get; }

    internal SourceVertex(GraphPeer graph, string key, SluiceCodec<T> codec)
    {
        _graph = graph;
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        Key = key;
        Id = new VertexId(graph.Self, key);
    }

    /// <summary>Set the value and invalidate every mirror across the federation.</summary>
    public void Set(T value) => _graph.SetOwned(Key, _codec.Serialize(value));

    /// <summary>The current value (a direct local read — this participant owns it).</summary>
    public T Current => _graph.Get(Id, _codec).Value;
}

/// <summary>
/// A self-refreshing typed view of any vertex — the federated analogue of <see cref="MirroredState{T}"/>. It runs
/// the read → await-invalidation → refetch loop on its own background thread (never the channel pump), so a remote
/// fetch can block safely, and raises <see cref="Updated"/> with the new value every time the owner changes the
/// vertex. Updates are version-monotonic: an out-of-order or stale refetch never moves <see cref="Current"/>
/// backwards.
/// </summary>
public sealed class ObservedVertex<T> : IDisposable
{
    private readonly GraphPeer _graph;
    private readonly SluiceCodec<T> _codec;
    private readonly IDisposable _registration;
    private readonly SemaphoreSlim _wake = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private readonly object _gate = new();

    public VertexId Id { get; }
    public T Current { get; private set; } = default!;
    public long Version { get; private set; } = -1;
    public Reach Reach { get; private set; }
    public bool Exists { get; private set; }

    /// <summary>Raised with the new value whenever the vertex is (re)read — including the initial read.</summary>
    public event Action<T>? Updated;

    internal ObservedVertex(GraphPeer graph, VertexId id, SluiceCodec<T> codec)
    {
        _graph = graph;
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        Id = id;
        // The pump thread only signals; the fetch happens on our loop thread (no reentrant block on the pump).
        _registration = graph.RegisterObserver(id, _ => SafeWake());
        _loop = Task.Run(RunAsync);
    }

    // The signal arrives on the channel pump thread; if we are being disposed concurrently the semaphore may
    // already be gone. Never let that throw back into the pump.
    private void SafeWake()
    {
        try { _wake.Release(); }
        catch (ObjectDisposedException) { }
        catch (SemaphoreFullException) { }
    }

    private async Task RunAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            bool settled = Refresh();
            try
            {
                // Settled (the owner answered): wait for the next invalidation. Unsettled (the request was lost):
                // wait only briefly, then retry — an invalidation may never come for a value that already changed.
                if (settled) await _wake.WaitAsync(ct).ConfigureAwait(false);
                else await _wake.WaitAsync(RetryDelayMs, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
            while (_wake.Wait(0)) { }   // collapse a burst of invalidations into a single refetch
        }
    }

    private const int RetryDelayMs = 200;

    private bool Refresh()
    {
        Computed<T> c;
        GraphPeer.BeginRead();
        try { c = _graph.Get(Id, _codec, _cts.Token); }
        catch (OperationCanceledException) { return true; }
        bool settled = !GraphPeer.LastReadWasLost;

        lock (_gate)
        {
            // Version-monotonic: ignore a refetch that is older than what we already have (unless we had nothing).
            if (Version >= 0 && c.Exists && c.Version < Version) return settled;
            Current = c.Value;
            Version = c.Version;
            Reach = c.Reach;
            Exists = c.Exists;
        }
        Updated?.Invoke(Current);
        return settled;
    }

    public void Dispose()
    {
        _registration.Dispose();
        _cts.Cancel();
        _wake.Release();   // unblock the loop so it can observe cancellation
        try { _loop.Wait(TimeSpan.FromSeconds(2)); } catch { /* cancelled */ }
        _cts.Dispose();
        _wake.Dispose();
    }
}

/// <summary>
/// A computed vertex this participant owns, derived from dependencies that may live on other participants. It
/// recomputes whenever any dependency is invalidated — re-reading each dep (routing local or remote as needed) on
/// its own background thread — then publishes its result as an owned vertex, which invalidates downstream mirrors.
/// That is the transitive step that makes the whole supergraph reactive: a change to a leaf propagates through
/// every computed that (transitively) depends on it, across participants.
/// </summary>
public sealed class ComputedVertex<T> : IDisposable
{
    private readonly GraphPeer _graph;
    private readonly SluiceCodec<T> _codec;
    private readonly Func<T> _compute;
    private readonly List<IDisposable> _registrations = new();
    private readonly SemaphoreSlim _wake = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    public VertexId Id { get; }
    public string Key { get; }
    public T Current { get; private set; } = default!;

    /// <summary>Raised with the new value each time this vertex recomputes.</summary>
    public event Action<T>? Updated;

    internal ComputedVertex(GraphPeer graph, string key, SluiceCodec<T> codec, Func<T> compute,
        VertexId[] dependencies)
    {
        _graph = graph;
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        _compute = compute ?? throw new ArgumentNullException(nameof(compute));
        Key = key;
        Id = new VertexId(graph.Self, key);
        foreach (var dep in dependencies)
            _registrations.Add(graph.RegisterObserver(dep, _ => SafeWake()));
        _loop = Task.Run(RunAsync);
    }

    private void SafeWake()
    {
        try { _wake.Release(); }
        catch (ObjectDisposedException) { }
        catch (SemaphoreFullException) { }
    }

    private async Task RunAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            bool settled = Recompute();
            try
            {
                // If every dependency read was answered, wait for the next invalidation; if a dep read was lost,
                // retry shortly so a dropped fetch can't leave this computed (and everything downstream) stale.
                if (settled) await _wake.WaitAsync(ct).ConfigureAwait(false);
                else await _wake.WaitAsync(RetryDelayMs, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
            while (_wake.Wait(0)) { }   // collapse a burst into one recompute
        }
    }

    private const int RetryDelayMs = 200;

    private bool Recompute()
    {
        T result;
        GraphPeer.BeginRead();
        try { result = _compute(); }
        catch (OperationCanceledException) { return true; }
        catch { return false; }   // a dependency read threw — retry shortly
        bool settled = !GraphPeer.LastReadWasLost;   // a dep fetch was lost → not settled, retry
        Current = result;
        _graph.SetOwned(Key, _codec.Serialize(result));   // publish → invalidates downstream mirrors federation-wide
        Updated?.Invoke(result);
        return settled;
    }

    public void Dispose()
    {
        foreach (var r in _registrations) r.Dispose();
        _cts.Cancel();
        _wake.Release();
        try { _loop.Wait(TimeSpan.FromSeconds(2)); } catch { /* cancelled */ }
        _cts.Dispose();
        _wake.Dispose();
    }
}
