namespace Sluice.Fusion;

/// <summary>The lifecycle state of a mirrored value — mirrors Fusion's <c>ConsistencyState</c>.</summary>
public enum ConsistencyState
{
    /// <summary>A fresh fetch is in flight.</summary>
    Computing = 0,
    /// <summary>The cached value is current.</summary>
    Consistent = 1,
    /// <summary>The origin signalled a change; the value is stale and will be refetched on next access.</summary>
    Invalidated = 2,
}

/// <summary>
/// An immutable snapshot of a mirrored value — the Sluice analogue of Fusion's <c>IComputed&lt;T&gt;</c>. It
/// carries the value, the origin's version, and a consistency state. When the origin invalidates the key, the
/// snapshot transitions to <see cref="ConsistencyState.Invalidated"/>, fires <see cref="Invalidated"/>, and
/// completes <see cref="WhenInvalidated"/>; the mirror then produces a new snapshot on the next read. Like
/// Fusion, the snapshot itself never mutates its value — an "update" is always a new snapshot.
/// </summary>
public sealed class SluiceComputed
{
    private readonly TaskCompletionSource _invalidated =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public string Key { get; }
    public byte[] Value { get; }
    public long Version { get; }
    public ConsistencyState State { get; private set; }

    internal SluiceComputed(string key, byte[] value, long version, ConsistencyState state = ConsistencyState.Consistent)
    {
        Key = key;
        Value = value;
        Version = version;
        State = state;
    }

    public bool IsConsistent => State == ConsistencyState.Consistent;

    /// <summary>Raised once when this snapshot is invalidated.</summary>
    public event Action? Invalidated;

    /// <summary>Completes when this snapshot is invalidated (immediately if already invalidated).</summary>
    public Task WhenInvalidated => _invalidated.Task;

    internal void Invalidate()
    {
        if (State == ConsistencyState.Invalidated) return;
        State = ConsistencyState.Invalidated;
        Invalidated?.Invoke();
        _invalidated.TrySetResult();
    }
}
