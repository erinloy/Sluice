namespace Sluice;

/// <summary>
/// The daemon-side acceptor that turns Sluice's point-to-point <see cref="ShmFrameChannel"/> into a
/// multiplexed server: many client processes connect, each gets its own duplex channel, and one daemon
/// services them all. This is the Sluice analogue of <c>NamedPipeServerStream.WaitForConnection</c> — the
/// rendezvous a frame-based daemon (e.g. an LSP multiplexer holding one language server) needs.
///
/// <para>
/// Protocol: the listener owns a small accept ring. A connecting client mints a unique id, creates its
/// per-connection rings, then posts the id; <see cref="Accept"/> reads the id, opens those rings, and returns
/// the server side of the channel. Per-connection routing is the application's job (just as it is over named
/// pipes) — the listener only hands you fresh channels.
/// </para>
/// </summary>
public sealed class ShmFrameListener : IDisposable
{
    private readonly string _endpoint;
    private readonly long _capacity;
    private readonly ShmRing _accept;

    public ShmFrameListener(string endpoint, long capacity = 1 << 20)
    {
        _endpoint = endpoint;
        _capacity = capacity;
        _accept = ShmRing.Create(ShmFrameChannel.AcceptName(endpoint), 64 * 1024);
    }

    /// <summary>
    /// Block until a client connects, then return the server side of its duplex channel. Throws
    /// <see cref="OperationCanceledException"/> if cancelled.
    /// </summary>
    public IFrameChannel Accept(CancellationToken ct = default)
    {
        while (true)
        {
            if (!_accept.WaitToRead(ct)) throw new OperationCanceledException(ct);
            if (_accept.TryRead(out var span))
            {
                long connId = BitConverter.ToInt64(span);
                _accept.AdvanceRead();
                var c2s = ShmRing.Open(ShmFrameChannel.ConnName(_endpoint, connId, "c2s"));
                var s2c = ShmRing.Open(ShmFrameChannel.ConnName(_endpoint, connId, "s2c"));
                return ShmFrameChannel.Server(c2s, s2c);
            }
        }
    }

    public void Dispose() => _accept.Dispose();
}
