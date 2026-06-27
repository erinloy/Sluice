using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sluice;

/// <summary>Delivery semantics for a multicast ring.</summary>
public enum DeliveryMode
{
    /// <summary>No message is dropped: a producer waits until the slowest registered subscriber has passed
    /// the slot it wants to reuse. Correct for commands/events you must not lose (but a stalled subscriber
    /// backpressures everyone — use only with well-behaved consumers).</summary>
    Reliable = 0,

    /// <summary>A producer never waits on consumers: it overwrites the oldest slot. A subscriber that falls
    /// more than a ring behind detects the gap (its slot was overwritten by a newer sequence) and resyncs
    /// forward, dropping what it missed. Lowest latency, crash-robust — market-data / telemetry style.</summary>
    Lossy = 1,
}

/// <summary>
/// A many-to-many multicast ring in shared memory — the multi-way core of Sluice. It is a Disruptor-style
/// <i>sequenced</i> ring: producers claim a globally-monotonic sequence with a single interlocked increment,
/// write their slot, then stamp the slot's sequence as the publish barrier. Every subscriber keeps its own
/// cursor and reads <b>every</b> message <b>in place</b> (a span straight into the shared pages). The same
/// per-slot stamp doubles as the lap detector that makes lossy delivery possible.
///
/// <para>
/// One producer ⇒ broadcast (1→N). Many producers ⇒ a full bus (N↔N) — the interlocked claim makes
/// multi-producer safe with no lock. Slot count must be a power of two; each slot is fixed size and holds
/// <c>[long sequence][int length][payload…]</c>.
/// </para>
///
/// <para><b>Platform:</b> cross-platform via <see cref="ShmMap"/> — a named map on Windows, a
/// <c>/dev/shm</c> file-backed map on Linux. The creation-race in <see cref="CreateOrAttach"/> is arbitrated by
/// <see cref="MemoryMappedFile.CreateNew(string,long)"/> on Windows and by an atomic <see cref="FileMode.CreateNew"/>
/// on Unix.</para>
/// </summary>
public sealed unsafe class ShmMulticast : IDisposable
{
    private const long Magic = 0x_5300_4D00_5500_4C02; // "S M U L" + version
    private const long FreeCell = long.MinValue;        // an unregistered subscriber slot
    private const int CursorPos = 0;                    // producer claim counter (last claimed seq)
    private const int ModePos = 64;
    private const int SlotCountPos = 68;
    private const int SlotSizePos = 72;
    private const int MaxConsumersPos = 76;
    private const int MagicPos = 80;
    private const int ConsumerCellsPos = 128;           // MaxConsumers × 8 bytes follow

    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _view;
    private readonly string? _backingFile;       // non-null on Unix; the creator unlinks it on dispose
    private readonly bool _ownsFile;
    private byte* _base;

    private readonly int _slotCount;
    private readonly int _slotSize;
    private readonly int _maxConsumers;
    private readonly long _mask;
    private readonly int _slotsBase;
    public DeliveryMode Mode { get; }

    /// <summary>Largest payload a slot can carry (slot minus the 12-byte slot header).</summary>
    public int MaxPayload => _slotSize - 12;

    private ShmMulticast(MemoryMappedFile mmf, long mapSize, int slotCount, int slotSize, int maxConsumers,
        DeliveryMode mode, string? backingFile = null, bool ownsFile = false)
    {
        _mmf = mmf;
        _backingFile = backingFile;
        _ownsFile = ownsFile;
        _slotCount = slotCount;
        _slotSize = slotSize;
        _maxConsumers = maxConsumers;
        _mask = slotCount - 1;
        Mode = mode;
        _slotsBase = Align64(ConsumerCellsPos + maxConsumers * 8);
        _view = mmf.CreateViewAccessor(0, mapSize, MemoryMappedFileAccess.ReadWrite);
        byte* p = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref p);
        _base = p;
    }

    /// <summary>Create (or open-and-reset) a multicast ring. <paramref name="slotCount"/> is rounded up to a
    /// power of two; <paramref name="maxPayload"/> sets the fixed slot capacity.</summary>
    public static ShmMulticast Create(string name, int maxPayload, int slotCount = 1024,
        DeliveryMode mode = DeliveryMode.Lossy, int maxConsumers = 64)
    {
        slotCount = NextPow2(slotCount);
        int slotSize = Align8(12 + maxPayload);
        long mapSize = Align64(ConsumerCellsPos + maxConsumers * 8) + (long)slotCount * slotSize;
        var mmf = ShmMap.OpenOrCreate(name, mapSize, out var backingFile);
        var r = new ShmMulticast(mmf, mapSize, slotCount, slotSize, maxConsumers, mode,
            backingFile, ownsFile: backingFile is not null);
        r.Init();
        return r;
    }

    /// <summary>
    /// Attach to a multicast ring by name, creating it if this is the first participant. Exactly one process
    /// wins the creation race and initialises the header with the given geometry; everyone else attaches and
    /// inherits it (so the creator's parameters are authoritative). This is what lets any peer in a bus start
    /// first without a coordinator.
    /// </summary>
    public static ShmMulticast CreateOrAttach(string name, int maxPayload, int slotCount = 1024,
        DeliveryMode mode = DeliveryMode.Lossy, int maxConsumers = 64)
    {
        slotCount = NextPow2(slotCount);
        int slotSize = Align8(12 + maxPayload);
        long mapSize = Align64(ConsumerCellsPos + maxConsumers * 8) + (long)slotCount * slotSize;
        try
        {
            var mmf = ShmMap.CreateNew(name, mapSize, out var backingFile);
            var r = new ShmMulticast(mmf, mapSize, slotCount, slotSize, maxConsumers, mode,
                backingFile, ownsFile: backingFile is not null);
            r.Init();   // we are the creator
            return r;
        }
        catch (IOException)
        {
            // Someone else won the creation race — inherit their geometry. They may not have stamped the
            // magic yet, so retry briefly until the header is initialised.
            var spin = new SpinWait();
            for (int attempt = 0; ; attempt++)
            {
                try { return Open(name); }
                catch (Exception) when (attempt < 1000) { spin.SpinOnce(); }
            }
        }
    }

    /// <summary>Open an existing multicast ring; geometry + mode are read back from its header.</summary>
    public static ShmMulticast Open(string name)
    {
        var mmf = ShmMap.OpenExisting(name);
        int slotCount, slotSize, maxConsumers; DeliveryMode mode;
        using (var hdr = mmf.CreateViewAccessor(0, ConsumerCellsPos, MemoryMappedFileAccess.Read))
        {
            byte* p = null;
            hdr.SafeMemoryMappedViewHandle.AcquirePointer(ref p);
            try
            {
                if (Volatile.Read(ref Unsafe.AsRef<long>(p + MagicPos)) != Magic)
                    throw new InvalidOperationException($"'{name}' is not a Sluice multicast ring.");
                mode = (DeliveryMode)Unsafe.ReadUnaligned<int>(p + ModePos);
                slotCount = Unsafe.ReadUnaligned<int>(p + SlotCountPos);
                slotSize = Unsafe.ReadUnaligned<int>(p + SlotSizePos);
                maxConsumers = Unsafe.ReadUnaligned<int>(p + MaxConsumersPos);
            }
            finally { hdr.SafeMemoryMappedViewHandle.ReleasePointer(); }
        }
        long mapSize = Align64(ConsumerCellsPos + maxConsumers * 8) + (long)slotCount * slotSize;
        return new ShmMulticast(mmf, mapSize, slotCount, slotSize, maxConsumers, mode);
    }

    private void Init()
    {
        Volatile.Write(ref Ref<long>(CursorPos), -1);                  // no sequence claimed yet
        Unsafe.WriteUnaligned(_base + ModePos, (int)Mode);
        Unsafe.WriteUnaligned(_base + SlotCountPos, _slotCount);
        Unsafe.WriteUnaligned(_base + SlotSizePos, _slotSize);
        Unsafe.WriteUnaligned(_base + MaxConsumersPos, _maxConsumers);
        for (int i = 0; i < _maxConsumers; i++)
            Volatile.Write(ref ConsumerCell(i), FreeCell);
        // Slot sequence stamps init to (index - slotCount): the "round -1" value that makes the first
        // round's reuse-wait and first read both resolve correctly.
        for (int i = 0; i < _slotCount; i++)
            Volatile.Write(ref SlotSeq(i), i - _slotCount);
        Volatile.Write(ref Ref<long>(MagicPos), Magic);
    }

    // ---- header / slot accessors -------------------------------------------------------------------

    private ref T Ref<T>(int bytePos) where T : unmanaged => ref Unsafe.AsRef<T>(_base + bytePos);
    private ref long ConsumerCell(int i) => ref Unsafe.AsRef<long>(_base + ConsumerCellsPos + i * 8);
    private byte* Slot(long seq) => _base + _slotsBase + (int)(seq & _mask) * _slotSize;
    private ref long SlotSeq(long index) => ref Unsafe.AsRef<long>(_base + _slotsBase + (int)index * _slotSize);

    // ---- producer ----------------------------------------------------------------------------------

    private long _cachedGate = long.MinValue;

    /// <summary>
    /// Publish <paramref name="payload"/> to all current subscribers. In reliable mode this blocks (spin)
    /// until the slot can be reused; in lossy mode it never blocks on consumers. Returns the sequence number
    /// assigned to the message.
    /// </summary>
    public long Publish(ReadOnlySpan<byte> payload, CancellationToken ct = default)
    {
        if (payload.Length > MaxPayload)
            throw new ArgumentException($"payload {payload.Length} exceeds slot MaxPayload {MaxPayload}", nameof(payload));

        long seq = Interlocked.Increment(ref Ref<long>(CursorPos)); // unique, monotonic claim (first = 0)
        long wrapAt = seq - _slotCount;

        // Wait until the previous occupant of this slot has finished being written (serialises slot reuse).
        var spin = new SpinWait();
        while (Volatile.Read(ref SlotSeq(seq & _mask)) != wrapAt)
        {
            ct.ThrowIfCancellationRequested();
            spin.SpinOnce();
        }

        // Reliable: also wait until every registered subscriber has consumed past the slot we're reusing.
        if (Mode == DeliveryMode.Reliable && wrapAt >= 0)
        {
            while (_cachedGate < wrapAt)
            {
                _cachedGate = MinConsumerCursor();
                if (_cachedGate >= wrapAt) break;
                ct.ThrowIfCancellationRequested();
                spin.SpinOnce();
            }
        }

        byte* slot = Slot(seq);
        Unsafe.WriteUnaligned(slot + 8, payload.Length);
        payload.CopyTo(new Span<byte>(slot + 12, payload.Length));
        Volatile.Write(ref Unsafe.AsRef<long>(slot), seq);          // publish barrier (release)
        return seq;
    }

    private long MinConsumerCursor()
    {
        long min = long.MaxValue;
        for (int i = 0; i < _maxConsumers; i++)
        {
            long v = Volatile.Read(ref ConsumerCell(i));
            if (v != FreeCell && v < min) min = v;
        }
        return min; // long.MaxValue when nobody is registered → producer never gates
    }

    // ---- subscriber --------------------------------------------------------------------------------

    /// <summary>
    /// Register a new subscriber. It starts from the current head, so it sees messages published from now on.
    /// Each subscriber has its own cursor and reads every message independently.
    /// </summary>
    public Subscriber Subscribe()
    {
        long head = Volatile.Read(ref Ref<long>(CursorPos));
        for (int i = 0; i < _maxConsumers; i++)
        {
            if (Interlocked.CompareExchange(ref ConsumerCell(i), head, FreeCell) == FreeCell)
                return new Subscriber(this, i, head + 1);
        }
        throw new InvalidOperationException($"multicast ring is full ({_maxConsumers} subscribers).");
    }

    internal void Unsubscribe(int cell) => Volatile.Write(ref ConsumerCell(cell), FreeCell);

    /// <summary>A per-consumer read cursor over a <see cref="ShmMulticast"/>. Not thread-safe — one per reader.</summary>
    public sealed class Subscriber
    {
        private readonly ShmMulticast _ring;
        private readonly int _cell;
        private long _next;
        private long _pending;
        public long Dropped { get; private set; }

        internal Subscriber(ShmMulticast ring, int cell, long next)
        {
            _ring = ring;
            _cell = cell;
            _next = next;
        }

        /// <summary>
        /// Try to read the next message <b>in place</b>. Returns false if nothing new is available yet. On a
        /// lossy ring, if this subscriber was lapped, it resyncs to the oldest still-live message and counts
        /// the gap in <see cref="Dropped"/> before returning that message.
        /// </summary>
        public bool TryRead(out ReadOnlySpan<byte> payload)
        {
            while (true)
            {
                long index = _next & _ring._mask;
                long stamp = Volatile.Read(ref _ring.SlotSeq(index)); // acquire

                if (stamp == _next)
                {
                    byte* slot = _ring.Slot(_next);
                    int len = Unsafe.ReadUnaligned<int>(slot + 8);
                    payload = new ReadOnlySpan<byte>(slot + 12, len);
                    _pending = _next + 1;
                    return true;
                }

                if (stamp < _next)
                {
                    payload = default;   // not published yet
                    return false;
                }

                // stamp > _next: our slot was overwritten by a newer sequence — we were lapped (lossy only).
                long oldest = stamp - _ring._slotCount + 1; // the oldest sequence still live in the ring
                long resyncTo = oldest > _next ? oldest : _next;
                Dropped += resyncTo - _next;
                _next = resyncTo;
                // loop and retry at the resynced position
            }
        }

        /// <summary>
        /// Read the next message into a fresh array and advance — the safe path for <b>lossy</b> rings, where a
        /// fast producer could overwrite a slot mid-read. It copies, then re-validates the slot's sequence
        /// stamp; if the slot was clobbered during the copy it is counted as dropped and the read retries.
        /// Returns false if nothing new is available.
        /// </summary>
        public bool TryReadCopy(out byte[] message)
        {
            while (true)
            {
                long index = _next & _ring._mask;
                long before = Volatile.Read(ref _ring.SlotSeq(index)); // acquire

                if (before == _next)
                {
                    byte* slot = _ring.Slot(_next);
                    int len = Unsafe.ReadUnaligned<int>(slot + 8);
                    var buf = new byte[len];
                    new ReadOnlySpan<byte>(slot + 12, len).CopyTo(buf);

                    // Re-validate: if the stamp still names our sequence, the copy was clean.
                    if (Volatile.Read(ref _ring.SlotSeq(index)) == _next)
                    {
                        _next++;
                        Volatile.Write(ref _ring.ConsumerCell(_cell), _next - 1);
                        message = buf;
                        return true;
                    }
                    // Clobbered mid-copy → fall through to lap handling.
                }
                else if (before < _next)
                {
                    message = [];
                    return false; // not published yet
                }

                // Lapped (before > _next, or a clobbered copy): resync to the oldest live sequence.
                long head = Volatile.Read(ref _ring.SlotSeq(index));
                long oldest = head - _ring._slotCount + 1;
                long resyncTo = oldest > _next ? oldest : _next + 1;
                Dropped += resyncTo - _next;
                _next = resyncTo;
            }
        }

        /// <summary>Commit the read peeked by <see cref="TryRead"/>, advancing this subscriber's cursor.</summary>
        public void Advance()
        {
            _next = _pending;
            Volatile.Write(ref _ring.ConsumerCell(_cell), _next - 1); // publish progress for reliable gating
        }

        /// <summary>Spin until a message is available (max-throughput wait), then return; false if cancelled.</summary>
        /// <remarks>
        /// Multicast has no doorbell (a single auto-reset event can't correctly wake N broadcast subscribers, and
        /// a manual-reset one has no safe resetter), so the wait polls. Under active load a message lands during
        /// the spin phase, so latency is unaffected; a genuinely-idle subscriber falls through to a 1 ms-capped
        /// sleep instead of busy-spinning, matching <see cref="ShmRing"/>'s idle backoff.
        /// </remarks>
        public bool Wait(CancellationToken ct = default, int spinCount = 512)
        {
            var spin = new SpinWait();
            for (int i = 0; ; i++)
            {
                long index = _next & _ring._mask;
                if (Volatile.Read(ref _ring.SlotSeq(index)) >= _next) return true;
                if (ct.IsCancellationRequested) return false;
                if (i < spinCount) spin.SpinOnce();   // self-escalating active spin (spin → yield → sleep)
                else Thread.Sleep(1);                 // idle tail: cap at ~1 ms so a quiet subscriber doesn't peg a core
            }
        }

        /// <summary>Release this subscriber's slot so producers stop gating on it (reliable mode).</summary>
        public void Dispose() => _ring.Unsubscribe(_cell);
    }

    // ---- helpers -----------------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Align8(int n) => (n + 7) & ~7;
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Align64(int n) => (n + 63) & ~63;

    private static int NextPow2(int n)
    {
        if (n < 2) return 2;
        n--; n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
        return n + 1;
    }

    public void Dispose()
    {
        if (_base != null)
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _base = null;
        }
        _view.Dispose();
        _mmf.Dispose();
        // The creator unlinks the tmpfs backing file so it doesn't outlive the ring (Unix only).
        if (_ownsFile) ShmMap.Unlink(_backingFile);
    }
}
