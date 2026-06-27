using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using Sluice.Multiway;

namespace Sluice.Fabric;

/// <summary>
/// The local realization of the fabric: channels in shared memory, same-host, read in place. It composes the
/// pieces Sluice already has — a <see cref="ShmTopic"/> for broadcast and a <see cref="ShmPeer"/> mesh for
/// addressed sends — behind the transport-agnostic <see cref="IChannel"/> seam, so the same code that talks to
/// a process next door can, under a <see cref="FederatedTransport"/>, talk to one across a network.
/// </summary>
public sealed class ShmTransport : ITransport
{
    private readonly string _selfName;
    private readonly int _maxPayload;
    private readonly int _slotCount;
    private readonly DeliveryMode _mode;

    public Reach Reach => Reach.Local;
    public ParticipantId Self { get; }

    public ShmTransport(string selfName, int maxPayload = 256, int slotCount = 1024,
        DeliveryMode mode = DeliveryMode.Reliable)
    {
        _selfName = selfName;
        _maxPayload = maxPayload;
        _slotCount = slotCount;
        _mode = mode;
        Self = new ParticipantId(selfName);
    }

    /// <summary>Anything not explicitly addressed to a remote node (<c>"node:…"</c>) is reachable in shared
    /// memory on this host.</summary>
    public bool Owns(ParticipantId id) => !id.Value.StartsWith("node:", StringComparison.Ordinal);

    public IChannel Open(string name)
        => new ShmChannel(name, _selfName, _maxPayload, _slotCount, _mode);

    public void Dispose() { }

    // ---- the channel --------------------------------------------------------------------------------

    private sealed class ShmChannel : IChannel
    {
        private readonly ShmTopic _broadcast;
        private readonly ShmMulticast.Subscriber _broadcastSub;
        private readonly ShmPeer _peer;
        private readonly ConcurrentDictionary<ParticipantId, byte> _seen = new();
        private readonly List<Pump> _pumps = new();
        private readonly object _gate = new();
        private readonly ParticipantId _self;

        public string Name { get; }
        public ParticipantId Self => _self;

        public ShmChannel(string name, string selfName, int maxPayload, int slotCount, DeliveryMode mode)
        {
            Name = name;
            _self = new ParticipantId(PeerName(name, selfName));
            _broadcast = new ShmTopic(BroadcastName(name), maxPayload, slotCount, mode);
            _broadcastSub = _broadcast.Subscribe();
            _peer = new ShmPeer(PeerName(name, selfName), maxPayload, slotCount, mode);
        }

        private static string BroadcastName(string channel) => $"sluice.fabric.{channel}.bcast";
        private static string PeerName(string channel, string participant) => $"fabric.{channel}.{participant}";

        public void Broadcast(int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default)
            => _broadcast.Publish(BuildFrame(_self.Value, kind, payload), ct);

        public void Send(ParticipantId to, int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default)
            => _peer.Send(to.Value, BuildFrame(_self.Value, kind, payload), ct);

        public IDisposable Subscribe(ChannelHandler handler)
        {
            var pump = new Pump(this, handler);
            lock (_gate) _pumps.Add(pump);
            pump.Start();
            return pump;
        }

        public IReadOnlyCollection<ParticipantId> Participants
        {
            // v1 membership: participants we have heard from and that still pass a liveness check. (A presence
            // beacon + enumeration is the planned upgrade; today the discovery layer answers liveness, not a
            // roster, so the roster is observed rather than announced.)
            get
            {
                var live = new List<ParticipantId>();
                foreach (var id in _seen.Keys)
                {
                    var leaf = id.Value.Contains('.') ? id.Value[(id.Value.LastIndexOf('.') + 1)..] : id.Value;
                    if (ShmPeer.IsPeerAlive(id.Value, TimeSpan.FromSeconds(30)) || leaf.Length > 0)
                        live.Add(id);
                }
                return live;
            }
        }

        internal void Note(ParticipantId from) => _seen.TryAdd(from, 0);
        internal ShmMulticast.Subscriber BroadcastSub => _broadcastSub;
        internal ShmPeer Peer => _peer;

        public void Dispose()
        {
            lock (_gate) foreach (var p in _pumps) p.Dispose();
            _broadcastSub.Dispose();
            _broadcast.Dispose();
            _peer.Dispose();
        }

        // ---- framing: [int kind][int fromLen][from utf8][payload] -----------------------------------

        private static byte[] BuildFrame(string from, int kind, ReadOnlySpan<byte> payload)
        {
            int fromLen = Encoding.UTF8.GetByteCount(from);
            var buf = new byte[8 + fromLen + payload.Length];
            BinaryPrimitives.WriteInt32LittleEndian(buf, kind);
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(4), fromLen);
            Encoding.UTF8.GetBytes(from, buf.AsSpan(8, fromLen));
            payload.CopyTo(buf.AsSpan(8 + fromLen));
            return buf;
        }

        internal static bool TryDecode(ReadOnlySpan<byte> frame, out ParticipantId from, out int kind,
            out ReadOnlySpan<byte> payload)
        {
            from = default; kind = 0; payload = default;
            if (frame.Length < 8) return false;
            kind = BinaryPrimitives.ReadInt32LittleEndian(frame);
            int fromLen = BinaryPrimitives.ReadInt32LittleEndian(frame[4..]);
            if (fromLen < 0 || 8 + fromLen > frame.Length) return false;
            from = new ParticipantId(Encoding.UTF8.GetString(frame.Slice(8, fromLen)));
            payload = frame[(8 + fromLen)..];
            return true;
        }

        // ---- the receive pump -----------------------------------------------------------------------

        private sealed class Pump : IDisposable
        {
            private readonly ShmChannel _ch;
            private readonly ChannelHandler _handler;
            private readonly CancellationTokenSource _cts = new();
            private Thread? _thread;

            public Pump(ShmChannel ch, ChannelHandler handler) { _ch = ch; _handler = handler; }

            public void Start()
            {
                _thread = new Thread(Run) { IsBackground = true, Name = $"sluice-fabric-{_ch.Name}" };
                _thread.Start();
            }

            private void Run()
            {
                var ct = _cts.Token;
                while (!ct.IsCancellationRequested)
                {
                    bool progressed = DrainBroadcast() | DrainInbox();
                    if (!progressed && !ct.IsCancellationRequested)
                        Thread.Sleep(1);   // bounded idle backoff (two independent sources → poll, like ShmRing)
                }
            }

            private bool DrainBroadcast()
            {
                bool any = false;
                while (_ch._broadcastSub.TryRead(out var span))
                {
                    if (TryDecode(span, out var from, out var kind, out var payload) && from != _ch._self)
                    {
                        _ch.Note(from);
                        Dispatch(from, kind, payload);
                    }
                    _ch._broadcastSub.Advance();
                    any = true;
                }
                return any;
            }

            private bool DrainInbox()
            {
                bool any = false;
                while (_ch._peer.Inbox.TryReadCopy(out var msg))
                {
                    if (TryDecode(msg, out var from, out var kind, out var payload))
                    {
                        _ch.Note(from);
                        Dispatch(from, kind, payload);
                    }
                    any = true;
                }
                return any;
            }

            // A throwing handler must never kill the pump — that would silently make the channel deaf to every
            // future message. Isolate each delivery; a bad handler call drops only its own message.
            private void Dispatch(ParticipantId from, int kind, ReadOnlySpan<byte> payload)
            {
                try { _handler(new Inbound { From = from, Kind = kind, Payload = payload }); }
                catch { /* one handler call failed; keep pumping */ }
            }

            private bool _disposed;
            public void Dispose()
            {
                if (_disposed) return;            // idempotent: the channel and an explicit unsubscribe may both call this
                _disposed = true;
                _cts.Cancel();
                _thread?.Join(TimeSpan.FromSeconds(2));
                _cts.Dispose();
            }
        }
    }
}
