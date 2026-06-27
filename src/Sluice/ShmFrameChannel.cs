using System.Security.Cryptography;

namespace Sluice;

/// <summary>
/// A bidirectional <see cref="IFrameChannel"/> over two <see cref="ShmRing"/>s — one per direction. The
/// server side creates both rings; the client side opens them, swapped, so each side reads what the other
/// writes. This is the drop-in replacement for a duplex <c>NamedPipeStream</c>, but frame-native and
/// zero-copy on read.
///
/// <para>
/// One channel is point-to-point. A multiplexing daemon (one process serving many client connections) gives
/// each connection its own uniquely-named channel; pair this with a small accept/rendezvous step so a fresh
/// client and the daemon agree on that name. (That broker is intentionally not baked in here — it belongs to
/// the daemon's connection model.)
/// </para>
/// </summary>
public sealed class ShmFrameChannel : IFrameChannel
{
    private readonly ShmRing _inbound;   // we read
    private readonly ShmRing _outbound;  // we write

    private ShmFrameChannel(ShmRing inbound, ShmRing outbound)
    {
        _inbound = inbound;
        _outbound = outbound;
    }

    /// <summary>Create the server side of a connection: both rings are created here under <paramref name="name"/>.</summary>
    public static ShmFrameChannel CreateServerSide(string name, long capacity = 1 << 20)
    {
        var c2s = ShmRing.Create(name + ".c2s", capacity); // client → server: server reads it
        var s2c = ShmRing.Create(name + ".s2c", capacity); // server → client: server writes it
        return new ShmFrameChannel(inbound: c2s, outbound: s2c);
    }

    /// <summary>Connect the client side to a server-created connection of the same <paramref name="name"/>.</summary>
    public static ShmFrameChannel ConnectClientSide(string name)
    {
        var c2s = ShmRing.Open(name + ".c2s"); // client writes it
        var s2c = ShmRing.Open(name + ".s2c"); // client reads it
        return new ShmFrameChannel(inbound: s2c, outbound: c2s);
    }

    /// <summary>
    /// Connect to a daemon that is running a <see cref="ShmFrameListener"/> on <paramref name="endpoint"/>.
    /// The client mints a unique connection id, creates its per-connection rings, and announces itself on the
    /// listener's accept ring; the listener's next <see cref="ShmFrameListener.Accept"/> picks it up. Returns
    /// the client side of a fresh duplex channel.
    /// </summary>
    public static ShmFrameChannel Connect(string endpoint, long capacity = 1 << 20)
    {
        long connId = NewConnId();
        var c2s = ShmRing.Create(ConnName(endpoint, connId, "c2s"), capacity); // client writes
        var s2c = ShmRing.Create(ConnName(endpoint, connId, "s2c"), capacity); // client reads

        using (var accept = ShmRing.Open(AcceptName(endpoint)))
        using (var mtx = new Mutex(false, AcceptName(endpoint) + ".mtx"))
        {
            mtx.WaitOne();
            try { accept.SyncProducerCursor(); accept.Write(BitConverter.GetBytes(connId)); }
            finally { mtx.ReleaseMutex(); }
        }
        return new ShmFrameChannel(inbound: s2c, outbound: c2s);
    }

    // Server side of an accepted connection: it reads c2s and writes s2c (rings the client created).
    internal static ShmFrameChannel Server(ShmRing c2s, ShmRing s2c) => new(inbound: c2s, outbound: s2c);

    internal static string AcceptName(string endpoint) => $"sluice.{endpoint}.accept";
    internal static string ConnName(string endpoint, long connId, string dir) => $"sluice.{endpoint}.{connId:x}.{dir}";

    private static long NewConnId()
    {
        Span<byte> b = stackalloc byte[8];
        RandomNumberGenerator.Fill(b);
        return BitConverter.ToInt64(b) & long.MaxValue;
    }

    public void WriteFrame(ReadOnlySpan<byte> frame, CancellationToken ct = default) => _outbound.Write(frame, ct);
    public bool TryReadFrame(out ReadOnlySpan<byte> frame) => _inbound.TryRead(out frame);
    public void AdvanceFrame() => _inbound.AdvanceRead();
    public bool WaitForFrame(CancellationToken ct = default) => _inbound.WaitToRead(ct);

    public void Dispose()
    {
        _inbound.Dispose();
        _outbound.Dispose();
    }
}
