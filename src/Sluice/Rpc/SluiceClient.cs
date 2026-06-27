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

        _reqMutex.WaitOne();
        try
        {
            _requests.SyncProducerCursor();
            _requests.Write(_scratch.AsSpan(0, frameLen));
        }
        finally { _reqMutex.ReleaseMutex(); }
    }

    public void Dispose()
    {
        _responses.Dispose();
        _requests.Dispose();
        _reqMutex?.Dispose();
        ArrayPool<byte>.Shared.Return(_scratch);
    }
}
