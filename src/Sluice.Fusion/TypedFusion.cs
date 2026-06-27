namespace Sluice.Fusion;

/// <summary>
/// A serializer pair that turns a <typeparamref name="T"/> into the raw bytes the Fusion overlay carries and
/// back again. Sluice itself stays serialization-agnostic — plug in MemoryPack, MessagePack, or anything else.
/// The deserializer takes a <see cref="ReadOnlySpan{T}"/> so it can read straight off the mapped/cached bytes
/// with no intermediate copy, keeping the typed layer faithful to Sluice's zero-copy spirit.
/// </summary>
/// <example>
/// MemoryPack, the canonical pairing:
/// <code>
/// var codec = new SluiceCodec&lt;Quote&gt;(
///     MemoryPackSerializer.Serialize,
///     span =&gt; MemoryPackSerializer.Deserialize&lt;Quote&gt;(span)!);
/// </code>
/// </example>
public sealed class SluiceCodec<T>
{
    public delegate T Deserializer(ReadOnlySpan<byte> bytes);

    public Func<T, byte[]> Serialize { get; }
    public Deserializer Deserialize { get; }

    public SluiceCodec(Func<T, byte[]> serialize, Deserializer deserialize)
    {
        Serialize = serialize ?? throw new ArgumentNullException(nameof(serialize));
        Deserialize = deserialize ?? throw new ArgumentNullException(nameof(deserialize));
    }
}

/// <summary>
/// A typed immutable snapshot — the generic face of <see cref="SluiceComputed"/>. It carries the deserialized
/// <typeparamref name="T"/> value alongside the origin's version and consistency state, and forwards the
/// underlying snapshot's invalidation hooks so reactive flows compose exactly as on the byte API. The raw bytes
/// remain reachable via <see cref="Raw"/> for callers that want the zero-copy view.
/// </summary>
public sealed class SluiceComputed<T>
{
    private readonly SluiceComputed _inner;

    internal SluiceComputed(SluiceComputed inner, T value)
    {
        _inner = inner;
        Value = value;
    }

    public string Key => _inner.Key;
    public T Value { get; }
    public long Version => _inner.Version;
    public ConsistencyState State => _inner.State;
    public bool IsConsistent => _inner.IsConsistent;

    /// <summary>False when the origin has no such key (the fetch returned a miss).</summary>
    public bool Exists => _inner.Version >= 0;

    /// <summary>The underlying bytes, viewed in place — no copy.</summary>
    public ReadOnlySpan<byte> Raw => _inner.Value;

    /// <summary>Raised once when this snapshot is invalidated.</summary>
    public event Action? Invalidated
    {
        add => _inner.Invalidated += value;
        remove => _inner.Invalidated -= value;
    }

    /// <summary>Completes when this snapshot is invalidated (immediately if already invalidated).</summary>
    public Task WhenInvalidated => _inner.WhenInvalidated;
}

/// <summary>
/// The typed origin side — a <typeparamref name="T"/>-shaped face over <see cref="ModelHost"/>. It serializes
/// on <see cref="Set"/> via the supplied <see cref="SluiceCodec{T}"/> and publishes the same tiny
/// invalidations; the byte substrate (topic + RPC) is unchanged.
/// </summary>
public sealed class ModelHost<T> : IDisposable
{
    private readonly ModelHost _host;
    private readonly SluiceCodec<T> _codec;

    public string Model => _host.Model;

    public ModelHost(string model, SluiceCodec<T> codec)
    {
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        _host = new ModelHost(model);
    }

    /// <summary>Set a key and invalidate every mirror's cached copy of it.</summary>
    public void Set(string key, T value) => _host.Set(key, _codec.Serialize(value));

    public bool TryGet(string key, out T value)
    {
        if (_host.TryGet(key, out var bytes))
        {
            value = _codec.Deserialize(bytes);
            return true;
        }
        value = default!;
        return false;
    }

    public void Dispose() => _host.Dispose();
}

/// <summary>
/// The typed mirror side — a <typeparamref name="T"/>-shaped face over <see cref="ModelMirror"/>. <see cref="Get"/>
/// returns a <see cref="SluiceComputed{T}"/> whose value is deserialized from the in-place response bytes, and the
/// invalidate→refetch lifecycle is inherited verbatim from the byte mirror it owns.
/// </summary>
public sealed class ModelMirror<T> : IDisposable
{
    private readonly ModelMirror _mirror;
    private readonly SluiceCodec<T> _codec;

    public string Model => _mirror.Model;

    /// <summary>Raised (on the listener thread) when a cached key is invalidated by the origin.</summary>
    public event Action<string>? Invalidated
    {
        add => _mirror.Invalidated += value;
        remove => _mirror.Invalidated -= value;
    }

    public ModelMirror(string model, SluiceCodec<T> codec)
    {
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        _mirror = new ModelMirror(model);
    }

    /// <summary>The current typed snapshot: the cached value if consistent, otherwise a fresh fetch + deserialize.</summary>
    public SluiceComputed<T> Get(string key)
    {
        var inner = _mirror.Get(key);
        // Don't run the deserializer over a miss sentinel (Version < 0, empty bytes).
        T value = inner.Version >= 0 ? _codec.Deserialize(inner.Value) : default!;
        return new SluiceComputed<T>(inner, value);
    }

    /// <summary>Convenience: the current value for a key, or <c>default</c> if the origin has no such key.</summary>
    public T? Value(string key) => Get(key) is { Exists: true } c ? c.Value : default;

    public void Dispose() => _mirror.Dispose();
}

/// <summary>
/// A self-refreshing typed view of a single key — the generic face of <see cref="MirroredState"/>. It runs the
/// canonical read → await-invalidation → refetch loop and raises <see cref="Updated"/> with the new
/// <typeparamref name="T"/> every time the origin changes the key, so consumers get a typed push feed.
/// </summary>
public sealed class MirroredState<T> : IDisposable
{
    private readonly ModelMirror<T> _mirror;
    private readonly string _key;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    public T Current { get; private set; } = default!;
    public long Version { get; private set; } = -1;

    /// <summary>Raised with the new value whenever the key is (re)computed — including the initial read.</summary>
    public event Action<T>? Updated;

    public MirroredState(ModelMirror<T> mirror, string key)
    {
        _mirror = mirror;
        _key = key;
        _loop = Task.Run(LoopAsync);
    }

    private async Task LoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            var computed = _mirror.Get(_key);
            Current = computed.Value;
            Version = computed.Version;
            Updated?.Invoke(Current);
            try { await computed.WhenInvalidated.WaitAsync(_cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) { break; }
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        try { _loop.Wait(TimeSpan.FromSeconds(2)); } catch { /* cancelled */ }
        _cts.Dispose();
    }
}
