namespace Sluice.Fabric;

/// <summary>Where a participant lives relative to this process.</summary>
public enum Reach
{
    /// <summary>Same host: reachable through shared memory, read in place, zero-copy.</summary>
    Local,
    /// <summary>Another host or process group: reachable only over a network transport.</summary>
    Remote,
}

/// <summary>
/// A transport-agnostic address for a participant in a channel. The scheme is opaque to the fabric; a
/// <see cref="ITransport"/> decides which addresses it <see cref="ITransport.Owns"/>. The convention Sluice's
/// own transports use is <c>"shm:&lt;name&gt;"</c> for a same-host participant and <c>"node:&lt;host&gt;/&lt;name&gt;"</c>
/// for a remote one, but a <see cref="FederatedTransport"/> routes purely by ownership, not by parsing.
/// </summary>
public readonly record struct ParticipantId(string Value)
{
    public override string ToString() => Value;
}

/// <summary>
/// A message delivered to a <see cref="ChannelHandler"/>. Local delivery hands <see cref="Payload"/> as a span
/// pointing straight into shared memory (no copy); remote delivery hands it over a materialized network buffer.
/// The handler sees the same shape either way — the whole point of the fabric is that local and remote
/// participants are indistinguishable at the seam.
/// </summary>
public readonly ref struct Inbound
{
    public ParticipantId From { get; init; }
    public int Kind { get; init; }
    public ReadOnlySpan<byte> Payload { get; init; }
}

/// <summary>Receives a message from a channel. The <paramref name="message"/> span is valid only for the
/// duration of the call (it may point into a shared-memory slot that is reused afterwards) — copy out anything
/// you need to retain.</summary>
public delegate void ChannelHandler(in Inbound message);

/// <summary>
/// The signaling seam — how a waiter is woken when a channel gains data or its membership changes. This is the
/// abstraction the rest of the system is built to honor: a <b>local</b> channel backs it with the shared-memory
/// doorbell (or its bounded polling fallback), a <b>remote</b> channel backs it with network readiness, and a
/// federated channel fans a wait across both. By contract <see cref="Wait"/> observes cancellation and may
/// return spuriously — always re-check your own condition after it returns.
/// </summary>
public interface ISignal
{
    /// <summary>Wake any current waiter(s): data or a membership change is available to observe.</summary>
    void Notify();

    /// <summary>Block until notified or cancelled. Returns false only on cancellation; a true return is a hint
    /// to re-check, not a guarantee of data.</summary>
    bool Wait(CancellationToken ct);
}

/// <summary>
/// A named, multi-participant medium: broadcast to everyone (1→N), an addressed send to one peer (→1), and a
/// live view of who is present. A channel does not care whether its participants are local or remote — that is
/// the transport's concern. This is the unit an RPC layer or a federation/sharding layer sits on top of.
/// </summary>
public interface IChannel : IDisposable
{
    string Name { get; }

    /// <summary>
    /// The address this channel publishes as — the <see cref="ParticipantId"/> that lands in
    /// <see cref="Inbound.From"/> on messages this channel sends, and the address a peer uses to
    /// <see cref="Send"/> back to it. A layer that needs to route a reply (a fetch, an RPC response, a federated
    /// graph's invalidation→refetch) reads this to know its own reply address. Under a
    /// <see cref="FederatedTransport"/> this is the local (same-host) facing identity; a remote peer addresses
    /// the node by the id it announced on the network reach.
    /// </summary>
    ParticipantId Self { get; }

    /// <summary>Publish to every participant on the channel.</summary>
    void Broadcast(int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default);

    /// <summary>Send addressed to a single participant, wherever it lives.</summary>
    void Send(ParticipantId to, int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default);

    /// <summary>Start receiving. The handler runs on a background pump until the returned token is disposed.</summary>
    IDisposable Subscribe(ChannelHandler handler);

    /// <summary>A snapshot of the participants currently believed present (by heartbeat/liveness).</summary>
    IReadOnlyCollection<ParticipantId> Participants { get; }
}

/// <summary>
/// Realizes channels over one medium. <see cref="ShmTransport"/> realizes them in shared memory (same-host,
/// zero-copy); a network transport realizes them over a socket/QUIC/etc.; and <see cref="FederatedTransport"/>
/// composes several so a single <see cref="IChannel"/> can span local and remote participants transparently,
/// routing each addressed send to whichever transport <see cref="Owns"/> the target.
/// </summary>
public interface ITransport : IDisposable
{
    /// <summary>The reach this transport provides — <see cref="Reach.Local"/> for shared memory.</summary>
    Reach Reach { get; }

    /// <summary>The identity this transport publishes as on the channels it opens.</summary>
    ParticipantId Self { get; }

    /// <summary>Open (or attach to) the named channel.</summary>
    IChannel Open(string name);

    /// <summary>True if this transport can reach <paramref name="id"/> — used by a federation router to pick a
    /// transport for an addressed send.</summary>
    bool Owns(ParticipantId id);
}
