using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sluice;

/// <summary>
/// A single-producer / single-consumer (SPSC) lock-free ring buffer that lives entirely inside a
/// memory-mapped file, so it can be shared between two processes on the same machine.
///
/// <para>
/// The defining property — and the reason this library exists — is <b>read-in-place</b>: the consumer
/// receives a <see cref="ReadOnlySpan{T}"/> pointing <i>directly into the shared memory pages</i>. There is
/// no copy out of the ring and no deserialize step on the read path. Contrast this with
/// <c>Cloudtoid.Interprocess</c>, which copies every message body out of the ring into a pooled/allocated
/// buffer before handing it back (it clears the slot and advances the read cursor immediately, so it
/// cannot hand out a stable view). Sluice keeps the slot alive until you call <see cref="AdvanceRead"/>,
/// which is what makes genuine zero-copy possible.
/// </para>
///
/// <para>
/// Framing: messages are laid out as <c>[int length][payload…]</c>, each frame 4-byte aligned. Cursors are
/// monotonic 64-bit byte counts (never wrapped); the physical position is <c>offset % Capacity</c>. A frame
/// never straddles the wrap boundary — when the tail of the buffer is too small, the producer writes a
/// <see cref="SkipMarker"/> filler that tells the consumer to jump to the next boundary.
/// </para>
///
/// <para>
/// Coordination is via two cursors in a cache-line-separated header plus an optional
/// <see cref="EventWaitHandle"/> "doorbell" so the consumer can block instead of spin. The ring itself is
/// correct without the doorbell (you can poll <see cref="TryRead"/>); the doorbell is purely a latency/CPU
/// trade-off.
/// </para>
///
/// <para><b>Platform:</b> cross-platform. On Windows the shared region is a named (page-file-backed)
/// memory-mapped file with a named <see cref="EventWaitHandle"/> doorbell. On Linux/Unix — where named maps
/// and named wait handles are unsupported in .NET — it is a file-backed map under <c>/dev/shm</c> (tmpfs, so
/// still RAM-resident) and the doorbell degrades to polling (the ring is correct without it). Named
/// <see cref="Mutex"/>, used by the discovery and MPSC layers, works on both. This is what lets Sluice run in
/// Azure Container Apps (Linux) as well as on Windows.</para>
/// </summary>
public sealed unsafe class ShmRing : IDisposable
{
    private const int HeaderSize = 256;          // control block; data region starts here
    private const int WriteOffsetPos = 0;        // producer cursor (cache line 0)
    private const int ReadOffsetPos = 64;        // consumer cursor (cache line 1)
    private const int CapacityPos = 128;         // immutable capacity, written at create
    private const int MagicPos = 136;            // format sentinel
    private const long Magic = 0x_5300_4C00_5500_4901; // "S L U I" tag + version nibble

    /// <summary>Length sentinel meaning "no message here; skip to the next wrap boundary".</summary>
    private const int SkipMarker = -1;

    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _view;
    private readonly EventWaitHandle? _doorbell;
    private readonly bool _ownsDoorbell;
    private readonly string? _backingFile;       // non-null on Unix; the owner unlinks it on dispose
    private readonly bool _ownsFile;
    private byte* _base;
    private readonly long _capacity;

    // Private cursor caches — each side owns its cursor and only re-reads the other side's cursor when its
    // local view says it must. This keeps the hot path off the shared cache line most of the time.
    private long _localWrite;
    private long _cachedRead;
    private long _localRead;
    private long _cachedWrite;

    // Pending advance for the peek/AdvanceRead protocol (single consumer, so a field is safe).
    private long _pendingAdvance;

    private ShmRing(MemoryMappedFile mmf, long capacity, EventWaitHandle? doorbell, bool ownsDoorbell,
        string? backingFile = null, bool ownsFile = false)
    {
        _mmf = mmf;
        _capacity = capacity;
        _doorbell = doorbell;
        _ownsDoorbell = ownsDoorbell;
        _backingFile = backingFile;
        _ownsFile = ownsFile;
        _view = mmf.CreateViewAccessor(0, HeaderSize + capacity, MemoryMappedFileAccess.ReadWrite);
        byte* p = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref p);
        _base = p;
    }

    /// <summary>Total bytes of payload-carrying data region (excludes the header).</summary>
    public long Capacity => _capacity;

    /// <summary>The largest single payload that can ever fit (a frame must fit contiguously after a wrap).</summary>
    public int MaxPayload => (int)(_capacity - 8);

    /// <summary>
    /// Create (or open-and-reset) the owner side of a ring under a system-global <paramref name="name"/>.
    /// The owner initialises the header. <paramref name="capacity"/> is rounded up to a 4-byte multiple.
    /// </summary>
    public static ShmRing Create(string name, long capacity, bool withDoorbell = true)
    {
        capacity = Align4(capacity);
        long size = HeaderSize + capacity;
        var mmf = ShmMap.OpenOrCreate(name, size, out var backingFile);
        // The doorbell is a named wait handle — Windows-only. On Unix, null => WaitToRead polls.
        EventWaitHandle? bell = withDoorbell ? ShmMap.CreateDoorbell(name) : null;
        var ring = new ShmRing(mmf, capacity, bell, ownsDoorbell: bell is not null,
            backingFile: backingFile, ownsFile: backingFile is not null);
        // Initialise header: zero cursors, stamp capacity + magic.
        Volatile.Write(ref ring.Cursor(WriteOffsetPos), 0);
        Volatile.Write(ref ring.Cursor(ReadOffsetPos), 0);
        Volatile.Write(ref ring.Cursor(CapacityPos), capacity);
        Volatile.Write(ref ring.Cursor(MagicPos), Magic);
        return ring;
    }

    /// <summary>
    /// Open an existing ring created by the owner. Capacity is read back from the shared header; the magic
    /// sentinel is verified.
    /// </summary>
    public static ShmRing Open(string name, bool withDoorbell = true)
    {
        var mmf = ShmMap.OpenExisting(name);
        // Peek the header to learn capacity, then re-map the full extent.
        long capacity;
        using (var hdr = mmf.CreateViewAccessor(0, HeaderSize, MemoryMappedFileAccess.Read))
        {
            byte* p = null;
            hdr.SafeMemoryMappedViewHandle.AcquirePointer(ref p);
            try
            {
                long magic = Volatile.Read(ref Unsafe.AsRef<long>(p + MagicPos));
                if (magic != Magic)
                    throw new InvalidOperationException($"Sluice ring '{name}' has bad magic 0x{magic:X} (not a Sluice ring or version mismatch).");
                capacity = Volatile.Read(ref Unsafe.AsRef<long>(p + CapacityPos));
            }
            finally { hdr.SafeMemoryMappedViewHandle.ReleasePointer(); }
        }
        EventWaitHandle? bell = withDoorbell ? ShmMap.OpenDoorbell(name) : null;
        return new ShmRing(mmf, capacity, bell, ownsDoorbell: false);
    }

    private ref long Cursor(int bytePos) => ref Unsafe.AsRef<long>(_base + bytePos);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long Align4(long n) => (n + 3) & ~3L;

    // ---- Producer side ------------------------------------------------------------------------------

    /// <summary>
    /// Try to publish <paramref name="payload"/> into the ring. Returns false (without blocking) if there is
    /// not enough free space right now — the caller decides whether to spin, back off, or drop.
    /// </summary>
    public bool TryWrite(ReadOnlySpan<byte> payload)
    {
        int payloadLen = payload.Length;
        if (payloadLen > MaxPayload)
            throw new ArgumentException($"payload {payloadLen} exceeds MaxPayload {MaxPayload}", nameof(payload));

        long frame = Align4(4 + payloadLen);
        long write = _localWrite;
        int pos = (int)(write % _capacity);
        long toEnd = _capacity - pos;            // multiple of 4, in [4, capacity]
        bool needSkip = toEnd < frame;
        long needed = (needSkip ? toEnd : 0) + frame;

        // Free-space check against the cached consumer cursor; only touch the shared cursor if we appear full.
        if (_capacity - (write - _cachedRead) < needed)
        {
            _cachedRead = Volatile.Read(ref Cursor(ReadOffsetPos));
            if (_capacity - (write - _cachedRead) < needed)
                return false;                    // genuinely full
        }

        if (needSkip)
        {
            WriteInt(pos, SkipMarker);
            write += toEnd;                       // jump to the wrap boundary
            pos = 0;
        }

        WriteInt(pos, payloadLen);
        payload.CopyTo(new Span<byte>(_base + HeaderSize + pos + 4, payloadLen));

        _localWrite = write + frame;
        Volatile.Write(ref Cursor(WriteOffsetPos), _localWrite);   // release: publishes the payload bytes
        _doorbell?.Set();
        return true;
    }

    /// <summary>
    /// Reload the producer cursor from shared memory. Single-producer use never needs this, but when several
    /// processes publish to one ring (serialized by an external mutex — the MPSC request ring in the RPC
    /// layer), each producer must resync the shared cursor under the lock before <see cref="TryWrite"/>,
    /// because the per-process cursor cache is otherwise stale.
    /// </summary>
    public void SyncProducerCursor() => _localWrite = Volatile.Read(ref Cursor(WriteOffsetPos));

    /// <summary>
    /// Publish <paramref name="payload"/>, spin-then-yield until space is available or <paramref name="ct"/>
    /// fires. Use when you require delivery and can tolerate backpressure.
    /// </summary>
    public void Write(ReadOnlySpan<byte> payload, CancellationToken ct = default)
    {
        var spin = new SpinWait();
        while (!TryWrite(payload))
        {
            ct.ThrowIfCancellationRequested();
            spin.SpinOnce();
        }
    }

    // ---- Consumer side ------------------------------------------------------------------------------

    /// <summary>
    /// Peek the next message <b>in place</b>. On success, <paramref name="payload"/> points directly into the
    /// shared memory pages and stays valid until <see cref="AdvanceRead"/> is called. Returns false if empty.
    /// </summary>
    /// <remarks>
    /// Usage is strictly: <c>if (TryRead(out var span)) { /* use span */ AdvanceRead(); }</c>. Do not retain
    /// the span past <see cref="AdvanceRead"/> — once advanced, the producer may overwrite that slot.
    /// </remarks>
    public bool TryRead(out ReadOnlySpan<byte> payload)
    {
        long read = _localRead;
        if (read == _cachedWrite)
        {
            _cachedWrite = Volatile.Read(ref Cursor(WriteOffsetPos));    // acquire: pairs with producer release
            if (read == _cachedWrite)
            {
                payload = default;
                return false;
            }
        }

        int pos = (int)(read % _capacity);
        int len = ReadInt(pos);
        long consumed = 0;
        if (len == SkipMarker)
        {
            long toEnd = _capacity - pos;
            read += toEnd;
            consumed += toEnd;
            pos = 0;
            len = ReadInt(pos);
        }

        payload = new ReadOnlySpan<byte>(_base + HeaderSize + pos + 4, len);
        _pendingAdvance = consumed + Align4(4 + len);
        return true;
    }

    /// <summary>Commit the read that <see cref="TryRead"/> peeked, freeing the slot for the producer.</summary>
    public void AdvanceRead()
    {
        _localRead += _pendingAdvance;
        _pendingAdvance = 0;
        Volatile.Write(ref Cursor(ReadOffsetPos), _localRead);          // release: frees space for producer
    }

    /// <summary>
    /// Block until at least one message is available (spin briefly, then wait on the doorbell), then return.
    /// Returns false only if cancelled. Requires the ring to have been opened with a doorbell.
    /// </summary>
    public bool WaitToRead(CancellationToken ct = default, int spinCount = 256)
    {
        for (int i = 0; i < spinCount; i++)
        {
            if (!IsEmpty) return true;
            if (ct.IsCancellationRequested) return false;
            Thread.SpinWait(1 << Math.Min(i, 8));
        }
        // Blocking phase. With a doorbell (Windows) we sleep until signalled; without one (Unix) we poll with
        // an escalating backoff capped at ~1 ms, so a missed signal can't hang and we don't peg a core.
        int idle = 0;
        while (IsEmpty)
        {
            if (ct.IsCancellationRequested) return false;
            if (_doorbell is not null)
            {
                _doorbell.WaitOne(20);                  // bounded so cancellation is still observed
                continue;
            }
            if (idle < 64) Thread.SpinWait(1 << Math.Min(idle, 10));
            else if (idle < 96) Thread.Yield();
            else Thread.Sleep(1);
            idle++;
        }
        return true;
    }

    /// <summary>True if there is no unread message (a cheap, possibly-stale check; re-checked under acquire).</summary>
    public bool IsEmpty
    {
        get
        {
            if (_localRead != _cachedWrite) return false;
            _cachedWrite = Volatile.Read(ref Cursor(WriteOffsetPos));
            return _localRead == _cachedWrite;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteInt(int pos, int value) => Unsafe.WriteUnaligned(_base + HeaderSize + pos, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int ReadInt(int pos) => Unsafe.ReadUnaligned<int>(_base + HeaderSize + pos);

    public void Dispose()
    {
        if (_base != null)
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _base = null;
        }
        _view.Dispose();
        _mmf.Dispose();
        if (_ownsDoorbell) _doorbell?.Dispose();
        // The owner unlinks the tmpfs backing file so it doesn't outlive the ring (Unix only).
        if (_ownsFile) ShmMap.Unlink(_backingFile);
    }
}
