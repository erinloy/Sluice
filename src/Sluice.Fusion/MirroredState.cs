namespace Sluice.Fusion;

/// <summary>
/// A self-refreshing view of a single mirrored key — the Sluice analogue of Fusion's <c>ComputedState&lt;T&gt;</c>.
/// It runs the canonical "read, await invalidation, refetch" loop: it exposes the latest value and raises
/// <see cref="Updated"/> every time the origin changes the key, so consumers get a push feed without polling.
/// </summary>
public sealed class MirroredState : IDisposable
{
    private readonly ModelMirror _mirror;
    private readonly string _key;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    public byte[] Current { get; private set; } = [];
    public long Version { get; private set; } = -1;

    /// <summary>Raised with the new value whenever the key is (re)computed — including the initial read.</summary>
    public event Action<byte[]>? Updated;

    public MirroredState(ModelMirror mirror, string key)
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
