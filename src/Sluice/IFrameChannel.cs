namespace Sluice;

/// <summary>
/// A bidirectional, frame-oriented channel — the transport seam a frame-based protocol (e.g. LSP, whose
/// messages are already <c>Content-Length</c>-delimited) plugs into so that different backends (a named pipe,
/// a Sluice shared-memory ring) are interchangeable. The read side is <b>frame-native and zero-copy</b>:
/// <see cref="TryReadFrame"/> hands back a span pointing into the underlying buffer, valid until
/// <see cref="AdvanceFrame"/>. This is the contract that preserves Sluice's read-in-place all the way up to
/// the consumer — a <c>Stream</c> adapter would re-introduce a copy on every read.
/// </summary>
public interface IFrameChannel : IDisposable
{
    /// <summary>Write one frame (the whole message body). Blocks with backpressure until it fits.</summary>
    void WriteFrame(ReadOnlySpan<byte> frame, CancellationToken ct = default);

    /// <summary>
    /// Peek the next inbound frame <b>in place</b>. The span is valid until <see cref="AdvanceFrame"/>; do not
    /// retain it past that call. Returns false if nothing is available right now.
    /// </summary>
    bool TryReadFrame(out ReadOnlySpan<byte> frame);

    /// <summary>Release the frame peeked by <see cref="TryReadFrame"/>, freeing the slot.</summary>
    void AdvanceFrame();

    /// <summary>Block (spin → doorbell) until an inbound frame is available; false if cancelled.</summary>
    bool WaitForFrame(CancellationToken ct = default);
}
