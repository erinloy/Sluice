using System.Buffers;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace Sluice.Rpc;

/// <summary>
/// The caller side of a Sluice RPC endpoint — the thin, short-lived process that talks to a running owner.
/// It creates its own per-client response ring (so replies are routed straight to it) and publishes requests
/// into the shared MPSC request ring, serialised across processes by a named mutex.
/// </summary>
public sealed class SluiceClient : IDisposable
{
    private readonly long _clientId;
    private readonly ShmRing _responses;   // this client is the single consumer (and creator)
    private readonly ShmRing _requests;    // opened as a producer; shared with other clients + owner
    private readonly Mutex? _reqMutex;     // serialises the many-producer request ring (null = exclusive)
    private readonly string _endpointName;   // diagnostics only — a timeout must name WHICH endpoint stalled
    private byte[] _scratch = ArrayPool<byte>.Shared.Rent(64 * 1024);

    public long ClientId => _clientId;

    /// <param name="endpoint">The endpoint name shared with the owner.</param>
    /// <param name="exclusiveProducer">
    /// Set true when this is the <b>only</b> process publishing to the endpoint right now (the common
    /// daemon + single-CLI case). It skips the cross-process producer mutex entirely — a kernel transition
    /// per send — so the request path is lock-free. Leave false when several client processes may publish
    /// concurrently; they are then serialised by a named mutex.
    /// </param>
    /// <param name="responseCapacity">Size of this client's private response ring.</param>
    public SluiceClient(string endpoint, bool exclusiveProducer = false, long responseCapacity = 1 << 20)
    {
        _endpointName = endpoint;
        _clientId = NewClientId();
        _responses = ShmRing.Create(RingNames.Response(endpoint, _clientId), responseCapacity);
        _requests = ShmRing.Open(RingNames.Request(endpoint));
        _reqMutex = exclusiveProducer ? null : new Mutex(false, RingNames.RequestMutex(endpoint));
    }

    private static long NewClientId()
    {
        Span<byte> b = stackalloc byte[8];
        RandomNumberGenerator.Fill(b);
        return BitConverter.ToInt64(b) & long.MaxValue; // keep it non-negative for clean hex names
    }

    /// <summary>Send a unary request and block until the correlated response arrives.</summary>
    public RpcResponse Send(int kind, ReadOnlySpan<byte> payload, CancellationToken ct = default)
    {
        var corr = Guid.NewGuid();
        WriteRequest(corr, kind, payload);

        while (true)
        {
            switch (TryConsume(corr, out var flags, out var body))
            {
                case Take.Matched:
                    return new RpcResponse((flags & RpcFlags.Ok) != 0, body);
                case Take.Empty:
                    if (!_responses.WaitToRead(ct)) throw new OperationCanceledException(ct);
                    break;
                // Take.Skipped: a stray frame for another in-flight call — keep draining.
            }
        }
    }

    /// <summary>
    /// Reads a unary response while it is still resident in the shared ring. <paramref name="response"/>
    /// is a view over the mapped pages — it is only valid for the duration of the callback; copy out
    /// anything you need to keep. <paramref name="state"/> threads caller context in without a closure.
    /// </summary>
    public delegate void ResponseReader<in TState>(bool ok, ReadOnlySpan<byte> response, TState state);

    /// <summary>
    /// Zero-allocation unary send: the correlated response is delivered to <paramref name="reader"/> as an
    /// in-place span over shared memory — no <see cref="RpcResponse"/>, no payload copy. The generic
    /// <typeparamref name="TState"/> lets the reader stay a static lambda (no per-call closure allocation),
    /// so the whole round-trip allocates nothing on the managed heap.
    /// </summary>
    public void Send<TState>(int kind, ReadOnlySpan<byte> payload, TState state,
        ResponseReader<TState> reader, CancellationToken ct = default)
    {
        var corr = Guid.NewGuid();
        WriteRequest(corr, kind, payload);

        while (true)
        {
            switch (TryConsumeInPlace(corr, state, reader))
            {
                case Take.Matched:
                    return;
                case Take.Empty:
                    if (!_responses.WaitToRead(ct)) throw new OperationCanceledException(ct);
                    break;
                // Take.Skipped: a stray frame for another in-flight call — keep draining.
            }
        }
    }

    /// <summary>Async convenience over the blocking <see cref="Send"/> (runs on the thread pool).</summary>
    public ValueTask<RpcResponse> SendAsync(int kind, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
        => new(Task.Run(() => Send(kind, payload.Span, ct), ct));

    /// <summary>
    /// Send a request and stream the correlated response elements until the owner completes the stream.
    /// </summary>
    public IEnumerable<byte[]> SendStream(int kind, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var corr = Guid.NewGuid();
        WriteRequest(corr, kind, payload.Span);

        while (true)
        {
            var take = TryConsume(corr, out var flags, out var body);
            if (take == Take.Empty)
            {
                if (!_responses.WaitToRead(ct)) yield break;
                continue;
            }
            if (take == Take.Skipped) continue;
            if ((flags & RpcFlags.StreamEnd) != 0) yield break;
            yield return body;
        }
    }

    private enum Take { Empty, Matched, Skipped }

    private Take TryConsume(Guid corr, out RpcFlags flags, out byte[] payload)
    {
        flags = RpcFlags.None;
        payload = Array.Empty<byte>();
        if (!_responses.TryRead(out var frame)) return Take.Empty;

        ref readonly RpcHeader h = ref MemoryMarshal.AsRef<RpcHeader>(frame);   // in-place reinterpret
        flags = h.Flags;
        bool match = h.CorrelationId == corr;
        if (match && (h.Flags & RpcFlags.StreamEnd) == 0)
            payload = frame.Slice(RpcHeader.Size).ToArray();                    // copy out across the return boundary
        _responses.AdvanceRead();
        return match ? Take.Matched : Take.Skipped;
    }

    // Zero-alloc sibling of TryConsume: hands the response span to the reader while it is still mapped,
    // then advances the ring. No ToArray, no out-byte[] crossing the return boundary.
    private Take TryConsumeInPlace<TState>(Guid corr, TState state, ResponseReader<TState> reader)
    {
        if (!_responses.TryRead(out var frame)) return Take.Empty;

        ref readonly RpcHeader h = ref MemoryMarshal.AsRef<RpcHeader>(frame);   // in-place reinterpret
        bool match = h.CorrelationId == corr;
        if (match)
            reader((h.Flags & RpcFlags.Ok) != 0, frame.Slice(RpcHeader.Size), state);
        _responses.AdvanceRead();
        return match ? Take.Matched : Take.Skipped;
    }

    private void WriteRequest(Guid corr, int kind, ReadOnlySpan<byte> payload)
    {
        int frameLen = RpcHeader.Size + payload.Length;
        if (_scratch.Length < frameLen)
        {
            ArrayPool<byte>.Shared.Return(_scratch);
            _scratch = ArrayPool<byte>.Shared.Rent(frameLen);
        }

        var header = new RpcHeader(corr, _clientId, kind, RpcFlags.None);
        MemoryMarshal.Write(_scratch, in header);
        payload.CopyTo(_scratch.AsSpan(RpcHeader.Size));

        // The cursor must always be synced from shared memory before writing: a fresh client process starts
        // with a zeroed local cursor while the shared ring has already advanced from prior producers
        // (sequential CLI invocations). The mutex is a separate concern — it serialises *concurrent*
        // producers; exclusive mode skips only the mutex, never the sync.
        if (_reqMutex is null)
        {
            _requests.SyncProducerCursor();
            _requests.Write(_scratch.AsSpan(0, frameLen));
            return;
        }

        // AbandonedMutexException MEANS WE OWN IT, AND CATCHING IT OUTSIDE THE try WAS A PERMANENT WEDGE.
        // A client process killed while holding this mutex (a harness hook timeout, a Ctrl-C, a crash) leaves it
        // abandoned. .NET then hands ownership to the next waiter and reports that by THROWING — so the old code,
        // which called WaitOne before entering the try, propagated out with the mutex HELD and never released. One
        // killed client took the endpoint down for every process on the machine, permanently, with no diagnostic.
        // The prior holder died mid-write, so the ring may carry a torn frame; that is the ring's own framing problem
        // and it self-corrects on the next read, whereas a lost mutex does not self-correct at all.
        //
        // 🛑 THE MUTEX IS NEVER HELD ACROSS A WAIT. THIS IS THE FIX FOR A FLEET-WIDE HANG, NOT A TIDY-UP.
        //
        // This used to be `WaitOne()` (unbounded) around `_requests.Write(...)` — and `ShmRing.Write` SPINS until the
        // ring has space, with a `ct` the call site never passed, i.e. forever. So a FULL request ring meant one writer
        // spinning inside the cross-process mutex while EVERY other process on the machine blocked on WaitOne with no
        // timeout. A lock held across an unbounded wait converts one slow reader into a machine-wide outage.
        //
        // 🩸 MEASURED, and the operator's workaround is the proof: hook 1 in a process took 2,395,182 ms (one observed
        // at 49 MINUTES) while hooks 2 and 3 in that SAME process took 185 ms and 126 ms — first-call-blocks,
        // everything-after-fast, which is the shape of contending for a held mutex exactly once per process. Erin:
        // "I am killing agentparticipant periodically to allow you to work." Killing the holder is what released it —
        // .NET hands ownership to the next waiter via AbandonedMutexException, so a kill genuinely cures it. That is a
        // held-lock signature, not a slow-service one.
        //
        // ⚖️ WHY NOT JUST BOUND THE WaitOne: that caps how long each victim waits and leaves the CAUSE — a writer
        // parked in a spin loop holding the lock — running. The lock now covers only a NON-BLOCKING TryWrite; if the
        // ring is full we RELEASE FIRST and back off outside the critical section, so a full ring can never block
        // anybody else. Both the acquire and the overall write are additionally bounded so a pathological peer
        // produces a fast, loud failure instead of a silent multi-minute stall.
        var deadline = Environment.TickCount64 + WriteTimeoutMs;
        var spin = new SpinWait();
        while (true)
        {
            bool owned;
            try { owned = _reqMutex.WaitOne(MutexAcquireMs); }
            catch (AbandonedMutexException) { owned = true; /* prior holder died — ownership is OURS */ }

            if (owned)
            {
                try
                {
                    _requests.SyncProducerCursor();
                    if (_requests.TryWrite(_scratch.AsSpan(0, frameLen)))
                        return;
                }
                finally { _reqMutex.ReleaseMutex(); }   // released BEFORE any wait, always
            }

            // Here we hold NOTHING: either the acquire timed out, or the ring was full and we let go.
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException(
                    $"Sluice request ring '{_endpointName}' did not accept a frame within {WriteTimeoutMs} ms. " +
                    "The ring is full (its reader is not draining) or the producer mutex is contended. This is a " +
                    "BOUNDED failure by design: the alternative — spinning inside the cross-process mutex — stalls " +
                    "every other process on this machine until someone kills the holder.");
            spin.SpinOnce();
        }
    }

    /// <summary>Cap on ONE attempt to take the producer mutex. Short: a long hold means the holder is in trouble, and
    /// re-trying the whole acquire is cheaper than queueing behind it.</summary>
    private const int MutexAcquireMs = 250;

    /// <summary>Cap on the WHOLE write, across retries. Generous enough that ordinary contention is invisible, finite
    /// so a wedged reader surfaces as a timeout rather than an indefinite hang.</summary>
    private const int WriteTimeoutMs = 5_000;

    public void Dispose()
    {
        _responses.Dispose();
        _requests.Dispose();
        _reqMutex?.Dispose();
        ArrayPool<byte>.Shared.Return(_scratch);
    }
}
