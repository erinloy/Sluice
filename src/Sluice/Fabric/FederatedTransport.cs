namespace Sluice.Fabric;

/// <summary>
/// Composes several transports into one so a single <see cref="IChannel"/> spans local and remote participants
/// transparently. An addressed <see cref="IChannel.Send"/> is routed to whichever member transport
/// <see cref="ITransport.Owns"/> the target; a <see cref="IChannel.Broadcast"/> fans out across every member,
/// so a message reaches same-host peers through shared memory (zero-copy) and far peers over the network in the
/// same call. This is the seam a federation/sharding layer and a cross-process RPC layer plug into: keep the
/// fast local path for co-located participants, and reach the rest of the network through the same channel.
///
/// <para>
/// The local transport is required (it is the zero-copy fast path and the identity this node publishes as).
/// Remote transports are injected — supply a socket/QUIC implementation of <see cref="ITransport"/> and remote
/// participants become reachable with no change to caller code. With no remote transport supplied this behaves
/// exactly like its local member, so adopting the fabric never costs you the local fast path.
/// </para>
/// </summary>
public sealed class FederatedTransport : ITransport
{
    private readonly ITransport _local;
    private readonly IReadOnlyList<ITransport> _remotes;
    private readonly IReadOnlyList<ITransport> _all;

    public Reach Reach => Reach.Local;          // this node participates locally; peers may be either reach
    public ParticipantId Self => _local.Self;

    public FederatedTransport(ITransport local, params ITransport[] remotes)
    {
        _local = local ?? throw new ArgumentNullException(nameof(local));
        _remotes = remotes ?? Array.Empty<ITransport>();
        _all = new List<ITransport>(1 + _remotes.Count) { _local }.Concat(_remotes).ToList();
    }

    public bool Owns(ParticipantId id) => _all.Any(t => t.Owns(id));

    /// <summary>Pick the transport that carries a participant — the local fast path wins ties.</summary>
    public ITransport RouteTo(ParticipantId id)
    {
        if (_local.Owns(id)) return _local;
        foreach (var r in _remotes) if (r.Owns(id)) return r;
        return _local; // unknown → assume local (it will surface there or nowhere), never silently drop to a guess
    }

    public IChannel Open(string name)
    {
        var channels = new List<IChannel>(_all.Count);
        foreach (var t in _all) channels.Add(t.Open(name));
        return new FederatedChannel(name, this, channels, _local.Self);
    }

    public void Dispose()
    {
        foreach (var t in _all) t.Dispose();
    }

    /// <summary>A channel that is the union of one channel per member transport. Broadcast fans across all;
    /// addressed send routes to the owner; subscribe merges every member's stream into one handler.</summary>
    private sealed class FederatedChannel : IChannel
    {
        private readonly FederatedTransport _owner;
        private readonly IReadOnlyList<IChannel> _members;   // index 0 is the local channel
        private readonly Dictionary<ITransport, IChannel> _byTransport;
        private readonly ParticipantId _self;

        public string Name { get; }

        public FederatedChannel(string name, FederatedTransport owner, IReadOnlyList<IChannel> members,
            ParticipantId self)
        {
            Name = name;
            _owner = owner;
            _members = members;
            _self = self;
            _byTransport = new Dictionary<ITransport, IChannel>();
            for (int i = 0; i < owner._all.Count; i++) _byTransport[owner._all[i]] = members[i];
        }

        public void Broadcast(int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default)
        {
            // Fan out to every reach. (Span can't cross the closure, so loop directly.)
            foreach (var m in _members) m.Broadcast(kind, payload, ct);
        }

        public void Send(ParticipantId to, int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default)
            => _byTransport[_owner.RouteTo(to)].Send(to, kind, payload, ct);

        public IDisposable Subscribe(ChannelHandler handler)
        {
            var subs = new List<IDisposable>(_members.Count);
            foreach (var m in _members) subs.Add(m.Subscribe(handler));
            return new CompositeDisposable(subs);
        }

        public IReadOnlyCollection<ParticipantId> Participants
        {
            get
            {
                var set = new HashSet<ParticipantId>();
                foreach (var m in _members) foreach (var p in m.Participants) set.Add(p);
                return set;
            }
        }

        public void Dispose() { foreach (var m in _members) m.Dispose(); }

        private sealed class CompositeDisposable(List<IDisposable> items) : IDisposable
        {
            public void Dispose() { foreach (var i in items) i.Dispose(); }
        }
    }
}
