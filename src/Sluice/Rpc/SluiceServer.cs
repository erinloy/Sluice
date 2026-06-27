using System.Buffers;
using System.Runtime.InteropServices;

namespace Sluice.Rpc;

/// <summary>
/// The owner/daemon side of a Sluice RPC endpoint. It consumes a single MPSC request ring (many client
/// processes publish, this one owner reads) and writes each reply into the caller's own per-client response
/// ring. Requests are dispatched to <see cref="SluiceHandler"/> <b>synchronously over the in-place span</b> —
/// that is the zero-copy, zero-deserialize hot path.
/// </summary>
public sealed class SluiceServer : IDisposable
{
    private readonly string _endpoint;
    private readonly ShmRing _requests;                 // owner is the single consumer
    private const int ResponseCacheCap = 256;
    private readonly Dictionary<long, ShmRing> _responses = new(); // clientId → that client's response ring (we publish)
    private readonly Queue<long> _evictionOrder = new();           // FIFO eviction to bound the cache
    private readonly SluiceHandler _handler;
    private readonly CancellationTokenSource _cts = new();
    private Thread? _loop;
    private byte[] _scratch = ArrayPool<byte>.Shared.Rent(64 * 1024);

    /// <summary>Raised when a single request fails to dispatch; the daemon keeps running.</summary>
    public event Action<Exception>? OnError;

    public SluiceServer(string endpoint, SluiceHandler handler, long requestCapacity = 1 << 20)
    {
        _endpoint = endpoint;
        _handler = handler;
        _requests = ShmRing.Create(RingNames.Request(endpoint), requestCapacity);
    }

    /// <summary>Launch the dispatch loop on a dedicated background thread. Stop it by disposing the server.</summary>
    public void Start()
    {
        if (_loop is not null) throw new InvalidOperationException("already started");
        _loop = new Thread(() => Run(_cts.Token)) { IsBackground = true, Name = $"sluice:{_endpoint}" };
        _loop.Start();
    }

    /// <summary>
    /// Run the dispatch loop until <paramref name="ct"/> is cancelled. Blocks the calling thread; prefer
    /// <see cref="Start"/> for a managed lifecycle.
    /// </summary>
    public void Run(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            if (!_requests.WaitToRead(ct)) break;
            // Drain everything currently available before going back to sleep — amortises the wake.
            while (_requests.TryRead(out var frame))
            {
                // A misbehaving or vanished client (e.g. its response ring is already gone) must never take
                // down the daemon — isolate each request.
                try { Dispatch(frame); }
                catch (Exception ex) { OnError?.Invoke(ex); }
                _requests.AdvanceRead();
            }
        }
    }

    private void Dispatch(ReadOnlySpan<byte> frame)
    {
        ref readonly RpcHeader h = ref MemoryMarshal.AsRef<RpcHeader>(frame);   // in-place reinterpret
        var payload = frame.Slice(RpcHeader.Size);
        var ctx = new RpcContext(this, h.ClientId, h.CorrelationId, h.Kind, payload);
        _handler(in ctx);
    }

    // Publish a reply frame ([RpcHeader][payload]) into the given client's response ring.
    internal void Publish(long clientId, Guid corr, RpcFlags flags, ReadOnlySpan<byte> payload)
    {
        if (!_responses.TryGetValue(clientId, out var ring))
        {
            // The client created its response ring before sending, so it exists while the client awaits.
            ring = ShmRing.Open(RingNames.Response(_endpoint, clientId));
            // Bound the cache: a persistent client stays hot (it keeps getting re-used), while a stream of
            // short-lived CLI clients — each with a unique id — cannot grow the cache without limit. An
            // evicted client simply pays one re-open on its next call.
            if (_responses.Count >= ResponseCacheCap && _evictionOrder.TryDequeue(out var old)
                && _responses.Remove(old, out var stale))
                stale.Dispose();
            _responses[clientId] = ring;
            _evictionOrder.Enqueue(clientId);
        }

        int frameLen = RpcHeader.Size + payload.Length;
        if (_scratch.Length < frameLen)
        {
            ArrayPool<byte>.Shared.Return(_scratch);
            _scratch = ArrayPool<byte>.Shared.Rent(frameLen);
        }

        var header = new RpcHeader(corr, clientId, 0, flags);
        MemoryMarshal.Write(_scratch, in header);
        payload.CopyTo(_scratch.AsSpan(RpcHeader.Size));
        ring.Write(_scratch.AsSpan(0, frameLen));
    }

    public void Dispose()
    {
        _cts.Cancel();
        _loop?.Join(TimeSpan.FromSeconds(5));   // ensure the loop stops touching the rings before we free them
        _cts.Dispose();
        _requests.Dispose();
        foreach (var r in _responses.Values) r.Dispose();
        ArrayPool<byte>.Shared.Return(_scratch);
    }
}

/// <summary>Handles one request, synchronously, with the request payload still mapped in shared memory.</summary>
public delegate void SluiceHandler(in RpcContext ctx);

/// <summary>
/// The per-request context handed to a <see cref="SluiceHandler"/>. A <c>ref struct</c> so the in-place
/// <see cref="Request"/> span cannot escape onto the heap or across an await. Reply via <see cref="Reply"/>
/// (unary) or <see cref="StreamItem"/>/<see cref="Complete"/> (streaming).
/// </summary>
public readonly ref struct RpcContext
{
    private readonly SluiceServer _server;
    public long ClientId { get; }
    public Guid CorrelationId { get; }
    public int Kind { get; }
    public ReadOnlySpan<byte> Request { get; }

    internal RpcContext(SluiceServer server, long clientId, Guid corr, int kind, ReadOnlySpan<byte> request)
    {
        _server = server;
        ClientId = clientId;
        CorrelationId = corr;
        Kind = kind;
        Request = request;
    }

    /// <summary>Send the single unary response for this request.</summary>
    public void Reply(ReadOnlySpan<byte> payload, bool ok = true)
        => _server.Publish(ClientId, CorrelationId, RpcFlags.Response | (ok ? RpcFlags.Ok : RpcFlags.None), payload);

    /// <summary>Send one element of a streamed response. Finish with <see cref="Complete"/>.</summary>
    public void StreamItem(ReadOnlySpan<byte> payload)
        => _server.Publish(ClientId, CorrelationId, RpcFlags.Response | RpcFlags.Ok | RpcFlags.StreamItem, payload);

    /// <summary>Terminate a streamed response.</summary>
    public void Complete()
        => _server.Publish(ClientId, CorrelationId, RpcFlags.Response | RpcFlags.Ok | RpcFlags.StreamEnd, ReadOnlySpan<byte>.Empty);
}
