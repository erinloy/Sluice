using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Sluice.Fabric;

/// <summary>
/// The remote realization of the fabric: channels over TCP, so participants on different hosts join the same
/// logical channel as those sharing memory locally. It is the network member a <see cref="FederatedTransport"/>
/// composes alongside a <see cref="ShmTransport"/> — co-located participants stay zero-copy in shared memory,
/// and everyone else is reached here, through the identical <see cref="IChannel"/> seam.
///
/// <para>
/// One transport multiplexes every channel over its connections: each frame carries its channel name, so a
/// single socket to a peer serves all the channels they share. Connections identify themselves with a hello
/// frame on connect, so an addressed <see cref="IChannel.Send"/> can find the right peer. This is a clean,
/// dependency-free socket transport (no TLS, no auto-reconnect yet — those are additive follow-ons); the wire
/// is length-prefixed and self-describing.
/// </para>
/// </summary>
public sealed class TcpTransport : ITransport
{
    private const int HelloKind = int.MinValue;   // reserved control frame: announces the sender's participant id

    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<string, Conn> _conns = new();          // remote participant id → connection
    private readonly ConcurrentDictionary<string, Handlers> _channels = new();   // channel name → its subscribers
    private readonly CancellationTokenSource _cts = new();
    private readonly List<Thread> _threads = new();

    public Reach Reach => Reach.Remote;
    public ParticipantId Self { get; }

    /// <summary>
    /// Create a TCP transport that listens on <paramref name="listenPort"/> and dials each known peer. The
    /// <paramref name="self"/> id is announced to peers and is how they address this node (use a
    /// <c>"node:…"</c> id so a federation router routes remote sends here).
    /// </summary>
    public TcpTransport(ParticipantId self, int listenPort,
        params (ParticipantId id, string host, int port)[] peers)
    {
        Self = self;
        _listener = new TcpListener(IPAddress.Loopback, listenPort);
        _listener.Start();
        Spawn(AcceptLoop, $"sluice-tcp-accept:{self.Value}");
        foreach (var p in peers)
        {
            var peer = p;   // capture
            Spawn(() => DialLoop(peer.host, peer.port), $"sluice-tcp-dial:{peer.id.Value}");
        }
    }

    public bool Owns(ParticipantId id) => id.Value.StartsWith("node:", StringComparison.Ordinal);

    public IChannel Open(string name) => new TcpChannel(this, name);

    private void Spawn(Action run, string name)
    {
        var t = new Thread(() => { try { run(); } catch (Exception) when (_cts.IsCancellationRequested) { } })
        { IsBackground = true, Name = name };
        lock (_threads) _threads.Add(t);
        t.Start();
    }

    // ---- connection setup ---------------------------------------------------------------------------

    private void AcceptLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            TcpClient client;
            try { client = _listener.AcceptTcpClient(); }
            catch (SocketException) { break; }
            catch (InvalidOperationException) { break; }
            OnSocket(client);
        }
    }

    private void DialLoop(string host, int port)
    {
        // Retry the initial connect a handful of times (the peer may still be starting), then give up. A dropped
        // connection is not re-dialed in this version.
        for (int attempt = 0; attempt < 50 && !_cts.IsCancellationRequested; attempt++)
        {
            try
            {
                var client = new TcpClient();
                client.Connect(host, port);
                OnSocket(client);
                return;
            }
            catch (SocketException)
            {
                if (_cts.Token.WaitHandle.WaitOne(100)) return;   // cancellable backoff
            }
        }
    }

    private void OnSocket(TcpClient client)
    {
        client.NoDelay = true;
        var conn = new Conn(client);
        SendHello(conn);                                          // announce who we are
        Spawn(() => ReadPump(conn), $"sluice-tcp-read:{Self.Value}");
    }

    private void SendHello(Conn conn) => WriteFrame(conn, "", HelloKind, Self.Value, ReadOnlySpan<byte>.Empty);

    // ---- the wire: [int frameLen][int kind][int chanLen][chan][int fromLen][from][payload] ----------

    private void WriteFrame(Conn conn, string channel, int kind, string from, ReadOnlySpan<byte> payload)
    {
        int chanLen = Encoding.UTF8.GetByteCount(channel);
        int fromLen = Encoding.UTF8.GetByteCount(from);
        int bodyLen = 4 + 4 + chanLen + 4 + fromLen + payload.Length;
        var buf = new byte[4 + bodyLen];
        int o = 0;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), bodyLen); o += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), kind); o += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), chanLen); o += 4;
        Encoding.UTF8.GetBytes(channel, buf.AsSpan(o, chanLen)); o += chanLen;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), fromLen); o += 4;
        Encoding.UTF8.GetBytes(from, buf.AsSpan(o, fromLen)); o += fromLen;
        payload.CopyTo(buf.AsSpan(o));

        try
        {
            lock (conn.WriteLock)
            {
                conn.Stream.Write(buf, 0, buf.Length);
                conn.Stream.Flush();
            }
        }
        catch (Exception) { Drop(conn); }   // broken pipe → drop the connection
    }

    private void ReadPump(Conn conn)
    {
        var stream = conn.Stream;
        var lenBuf = new byte[4];
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                stream.ReadExactly(lenBuf, 0, 4);
                int bodyLen = BinaryPrimitives.ReadInt32LittleEndian(lenBuf);
                if (bodyLen < 12 || bodyLen > 64 * 1024 * 1024) break;   // malformed / oversized
                var body = new byte[bodyLen];
                stream.ReadExactly(body, 0, bodyLen);
                Dispatch(conn, body);
            }
        }
        catch (Exception) { /* connection closed or error */ }
        finally { Drop(conn); }
    }

    private void Dispatch(Conn conn, byte[] body)
    {
        int o = 0;
        int kind = BinaryPrimitives.ReadInt32LittleEndian(body.AsSpan(o)); o += 4;
        int chanLen = BinaryPrimitives.ReadInt32LittleEndian(body.AsSpan(o)); o += 4;
        string channel = Encoding.UTF8.GetString(body, o, chanLen); o += chanLen;
        int fromLen = BinaryPrimitives.ReadInt32LittleEndian(body.AsSpan(o)); o += 4;
        var from = new ParticipantId(Encoding.UTF8.GetString(body, o, fromLen)); o += fromLen;

        if (kind == HelloKind)
        {
            conn.RemoteId = from.Value;
            _conns[from.Value] = conn;     // now addressable
            return;
        }

        if (_channels.TryGetValue(channel, out var handlers))
        {
            var payload = new ReadOnlySpan<byte>(body, o, body.Length - o);
            handlers.Invoke(new Inbound { From = from, Kind = kind, Payload = payload });
        }
    }

    private void Drop(Conn conn)
    {
        if (conn.RemoteId is { } id) _conns.TryRemove(KeyValuePair.Create(id, conn));
        conn.Dispose();
    }

    // ---- channel/transport plumbing used by TcpChannel ----------------------------------------------

    internal void Broadcast(string channel, int kind, ReadOnlySpan<byte> payload)
    {
        foreach (var conn in _conns.Values) WriteFrame(conn, channel, kind, Self.Value, payload);
    }

    internal void SendTo(string channel, ParticipantId to, int kind, ReadOnlySpan<byte> payload)
    {
        if (_conns.TryGetValue(to.Value, out var conn)) WriteFrame(conn, channel, kind, Self.Value, payload);
    }

    internal IDisposable AddHandler(string channel, ChannelHandler handler)
    {
        var handlers = _channels.GetOrAdd(channel, _ => new Handlers());
        return handlers.Add(handler);
    }

    internal IReadOnlyCollection<ParticipantId> ConnectedPeers()
    {
        var list = new List<ParticipantId>();
        foreach (var id in _conns.Keys) list.Add(new ParticipantId(id));
        return list;
    }

    public void Dispose()
    {
        _cts.Cancel();
        try { _listener.Stop(); } catch { /* ignore */ }
        foreach (var conn in _conns.Values) conn.Dispose();
        lock (_threads) foreach (var t in _threads) t.Join(TimeSpan.FromSeconds(1));
        _cts.Dispose();
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private sealed class Conn(TcpClient client) : IDisposable
    {
        public TcpClient Client { get; } = client;
        public NetworkStream Stream { get; } = client.GetStream();
        public object WriteLock { get; } = new();
        public string? RemoteId { get; set; }
        private int _disposed;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            try { Stream.Dispose(); } catch { /* ignore */ }
            try { Client.Dispose(); } catch { /* ignore */ }
        }
    }

    private sealed class Handlers
    {
        private readonly List<ChannelHandler> _list = new();
        private readonly object _gate = new();

        public IDisposable Add(ChannelHandler h)
        {
            lock (_gate) _list.Add(h);
            return new Remove(this, h);
        }

        public void Invoke(in Inbound msg)
        {
            // Snapshot under lock, invoke outside it (handlers may run arbitrary user code). A throwing handler
            // must not tear down the read pump / connection, so isolate each call.
            ChannelHandler[] snapshot;
            lock (_gate) snapshot = _list.ToArray();
            foreach (var h in snapshot)
            {
                try { h(msg); } catch { /* one handler failed; keep delivering */ }
            }
        }

        private sealed class Remove(Handlers owner, ChannelHandler h) : IDisposable
        {
            public void Dispose() { lock (owner._gate) owner._list.Remove(h); }
        }
    }

    private sealed class TcpChannel(TcpTransport t, string name) : IChannel
    {
        public string Name => name;

        /// <summary>The node id this transport announced — what remote peers see as <see cref="Inbound.From"/>
        /// and address their replies to.</summary>
        public ParticipantId Self => t.Self;

        public void Broadcast(int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default)
            => t.Broadcast(name, kind, payload);

        public void Send(ParticipantId to, int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default)
            => t.SendTo(name, to, kind, payload);

        public IDisposable Subscribe(ChannelHandler handler) => t.AddHandler(name, handler);

        public IReadOnlyCollection<ParticipantId> Participants => t.ConnectedPeers();

        public void Dispose() { }   // the transport owns the connections; closing a channel view is a no-op
    }
}
