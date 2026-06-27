using Sluice.Rpc;

namespace Sluice.Multiway;

/// <summary>
/// An addressed, full-duplex peer in a shared-memory mesh. Each peer owns an inbox ring; any peer can
/// <see cref="Send"/> a message addressed to another peer by name, and <see cref="TryReceive"/> messages
/// addressed to itself. Because a peer both sends and receives independently, communication is inherently
/// full-duplex and N-way — there is no client/server asymmetry.
/// </summary>
public sealed class ShmPeer : IDisposable
{
    private readonly int _maxPayload;
    private readonly int _slotCount;
    private readonly DeliveryMode _mode;
    private readonly ShmMulticast _inbox;
    private readonly ShmMulticast.Subscriber _self;
    private readonly Dictionary<string, ShmMulticast> _outboxes = new(StringComparer.Ordinal);
    private readonly object _outboxGate = new();   // Send may be called concurrently from many threads

    public string Name { get; }

    public ShmPeer(string name, int maxPayload = 256, int slotCount = 1024, DeliveryMode mode = DeliveryMode.Reliable)
    {
        Name = name;
        _maxPayload = maxPayload;
        _slotCount = slotCount;
        _mode = mode;
        _inbox = ShmMulticast.CreateOrAttach(InboxName(name), maxPayload, slotCount, mode);
        _self = _inbox.Subscribe();
        SluiceDiscovery.Heartbeat("peer." + name, slotCount);   // announce presence for discovery
    }

    private static string InboxName(string peer) => $"sluice.peer.{peer}.inbox";

    /// <summary>Is the named peer currently live (heartbeat fresh)?</summary>
    public static bool IsPeerAlive(string peer, TimeSpan staleAfter)
        => SluiceDiscovery.IsAlive("peer." + peer, staleAfter);

    /// <summary>
    /// Send a message addressed to <paramref name="toPeer"/>. The first send to a peer attaches to its inbox
    /// (cached thereafter). Multiple senders to the same inbox are safe — the ring's interlocked claim makes
    /// it multi-producer.
    /// </summary>
    public void Send(string toPeer, ReadOnlySpan<byte> payload, CancellationToken ct = default)
    {
        // Resolve (or attach) the outbox under a lock — the cache is a plain Dictionary and Send is hot from
        // multiple threads (e.g. a fabric channel's pump replying while a worker thread sends a request). The
        // ring's own Publish is lock-free, so it stays outside the lock; the lock only guards the cache lookup,
        // which is uncontended once the peer is warm.
        ShmMulticast ring;
        lock (_outboxGate)
        {
            if (!_outboxes.TryGetValue(toPeer, out ring!))
            {
                ring = ShmMulticast.CreateOrAttach(InboxName(toPeer), _maxPayload, _slotCount, _mode);
                _outboxes[toPeer] = ring;
            }
        }
        ring.Publish(payload, ct);
    }

    /// <summary>Try to take the next message addressed to this peer (safe copy; works in either mode).</summary>
    public bool TryReceive(out byte[] message) => _self.TryReadCopy(out message);

    /// <summary>Block (spin/yield) until a message is addressed to this peer, then return it; false if cancelled.</summary>
    public bool Receive(out byte[] message, CancellationToken ct = default)
    {
        if (_self.Wait(ct)) return _self.TryReadCopy(out message);
        message = [];
        return false;
    }

    /// <summary>The raw inbox subscription, for zero-copy in-place receives by advanced callers.</summary>
    public ShmMulticast.Subscriber Inbox => _self;

    public void Dispose()
    {
        _self.Dispose();
        _inbox.Dispose();
        lock (_outboxGate)
            foreach (var r in _outboxes.Values) r.Dispose();
    }
}
