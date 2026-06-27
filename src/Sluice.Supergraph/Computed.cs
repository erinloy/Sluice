using Sluice.Fabric;
using Sluice.Fusion;

namespace Sluice.Supergraph;

/// <summary>
/// An immutable snapshot of a supergraph vertex as one participant currently sees it — the federated analogue of
/// <see cref="SluiceComputed"/>. It carries the deserialized value, the owner's version, the
/// <see cref="Fabric.Reach"/> the value came from (was it a local in-place read or a remote fetch?), and a
/// consistency state. When the owner invalidates the vertex the snapshot transitions to
/// <see cref="ConsistencyState.Invalidated"/>, fires <see cref="Invalidated"/> and completes
/// <see cref="WhenInvalidated"/>; a fresh read then produces a new snapshot. Like Fusion, a snapshot never mutates
/// its value — an update is always a new snapshot.
/// </summary>
public sealed class Computed<T>
{
    private readonly TaskCompletionSource _invalidated =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public VertexId Id { get; }
    public T Value { get; }
    public long Version { get; }

    /// <summary>Where this value was read from — <see cref="Fabric.Reach.Local"/> (same host, read in place) or
    /// <see cref="Fabric.Reach.Remote"/> (fetched over the network).</summary>
    public Reach Reach { get; }

    public ConsistencyState State { get; private set; }

    /// <summary>False when the owner has no such vertex (the read was a miss).</summary>
    public bool Exists => Version >= 0;
    public bool IsConsistent => State == ConsistencyState.Consistent;

    /// <summary>The raw value bytes — the zero-copy view for callers that want the substrate directly.</summary>
    public ReadOnlyMemory<byte> Raw { get; }

    /// <summary>Raised once when this snapshot is invalidated.</summary>
    public event Action? Invalidated;

    /// <summary>Completes when this snapshot is invalidated (immediately if already invalidated).</summary>
    public Task WhenInvalidated => _invalidated.Task;

    internal Computed(VertexId id, T value, long version, Reach reach, ReadOnlyMemory<byte> raw,
        ConsistencyState state = ConsistencyState.Consistent)
    {
        Id = id;
        Value = value;
        Version = version;
        Reach = reach;
        Raw = raw;
        State = state;
        if (state == ConsistencyState.Invalidated) _invalidated.TrySetResult();
    }

    internal void Invalidate()
    {
        if (State == ConsistencyState.Invalidated) return;
        State = ConsistencyState.Invalidated;
        Invalidated?.Invoke();
        _invalidated.TrySetResult();
    }
}
