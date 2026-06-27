using System.IO.Pipes;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Cloudtoid.Interprocess;
using MemoryPack;
using MessagePack;
using Sluice.Rpc;

namespace Sluice.Benchmarks;

/// <summary>
/// End-to-end request → response latency for the same logical message across local-IPC transports. The
/// responder runs on a background thread; the payload still crosses the real shared-memory / pipe boundary,
/// so this faithfully measures transport + serialization cost (the only thing co-locating the threads removes
/// is a second OS process, which does not change the per-message work).
///
/// Sluice sends the payload raw — that is the zero-serialization design. Every other transport must encode a
/// message to carry the same bytes (and the responder decodes + re-encodes, as a real server would), so the
/// gap you see is exactly the cost Sluice removes.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class RoundTripBenchmarks
{
    private const int PayloadSize = 256;
    private byte[] _payload = [];

    // --- Sluice ---
    private SluiceServer _sluiceServer = null!;
    private SluiceClient _sluiceClient = null!;

    // --- Cloudtoid ---
    private QueueFactory _qf = null!;
    private IPublisher _ctReqPub = null!;
    private ISubscriber _ctRespSub = null!;
    private CancellationTokenSource _ctCts = null!;
    private Thread _ctMpThread = null!;
    private Thread _ctJsonThread = null!;
    private string _ctPath = "";

    // --- Named pipes ---
    private string _pipeMpName = "";
    private string _pipeJsonName = "";
    private NamedPipeClientStream _pipeMpClient = null!;
    private NamedPipeClientStream _pipeJsonClient = null!;
    private CancellationTokenSource _pipeCts = null!;
    private readonly byte[] _lenBuf = new byte[4];

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadSize];
        new Random(42).NextBytes(_payload);

        SetupSluice();
        SetupCloudtoid();
        SetupPipes();
    }

    // ============================ Sluice ============================

    private void SetupSluice()
    {
        var endpoint = "bench-" + Guid.NewGuid().ToString("N");
        _sluiceServer = new SluiceServer(endpoint, static (in RpcContext ctx) => ctx.Reply(ctx.Request));
        _sluiceServer.Start();
        Thread.Sleep(50);
        _sluiceClient = new SluiceClient(endpoint, exclusiveProducer: true);
    }

    [Benchmark(Baseline = true)]
    public int Sluice()
    {
        var resp = _sluiceClient.Send(1, _payload);
        return resp.Payload.Length;
    }

    // The zero-alloc receive path: the response is read in place over shared memory via the span callback,
    // so the round-trip allocates nothing on the managed heap (MemoryDiagnoser should report 0 B/op). The
    // static reader + int-by-ref state avoids a per-call closure.
    private static readonly SluiceClient.ResponseReader<int[]> s_lenReader =
        static (bool ok, ReadOnlySpan<byte> resp, int[] box) => box[0] = resp.Length;
    private readonly int[] _lenBox = new int[1];

    [Benchmark]
    public int Sluice_ZeroAlloc()
    {
        _sluiceClient.Send(1, _payload, _lenBox, s_lenReader);
        return _lenBox[0];
    }

    // ============================ Cloudtoid ============================

    private void SetupCloudtoid()
    {
        _ctPath = Path.Combine(Path.GetTempPath(), "sluice-bench", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_ctPath);
        _qf = new QueueFactory();
        _ctCts = new CancellationTokenSource();

        const long cap = 1 << 20;
        // MemoryPack echo responder over its own request/response pair.
        var mpReqSub = _qf.CreateSubscriber(new QueueOptions("bench-ct-mp-req", _ctPath, cap));
        var mpRespPub = _qf.CreatePublisher(new QueueOptions("bench-ct-mp-resp", _ctPath, cap));
        _ctMpThread = StartCloudtoidEcho(mpReqSub, mpRespPub, _ctCts.Token, json: false);

        // JSON echo responder.
        var jsonReqSub = _qf.CreateSubscriber(new QueueOptions("bench-ct-json-req", _ctPath, cap));
        var jsonRespPub = _qf.CreatePublisher(new QueueOptions("bench-ct-json-resp", _ctPath, cap));
        _ctJsonThread = StartCloudtoidEcho(jsonReqSub, jsonRespPub, _ctCts.Token, json: true);

        // Client endpoints (we reuse the MP pair for the MP benchmark and JSON pair for the JSON benchmark).
        _ctReqPub = _qf.CreatePublisher(new QueueOptions("bench-ct-mp-req", _ctPath, cap));
        _ctRespSub = _qf.CreateSubscriber(new QueueOptions("bench-ct-mp-resp", _ctPath, cap));
        _ctJsonReqPub = _qf.CreatePublisher(new QueueOptions("bench-ct-json-req", _ctPath, cap));
        _ctJsonRespSub = _qf.CreateSubscriber(new QueueOptions("bench-ct-json-resp", _ctPath, cap));
        Thread.Sleep(50);
    }

    private IPublisher _ctJsonReqPub = null!;
    private ISubscriber _ctJsonRespSub = null!;

    private Thread StartCloudtoidEcho(ISubscriber sub, IPublisher pub, CancellationToken ct, bool json)
    {
        var t = new Thread(() =>
        {
            while (!ct.IsCancellationRequested)
            {
                ReadOnlyMemory<byte> msg;
                try { msg = sub.Dequeue(ct); }
                catch (OperationCanceledException) { break; }

                // Decode then re-encode, as a real server handling the message would.
                byte[] reply;
                if (json)
                {
                    var m = JsonSerializer.Deserialize<JsonMsg>(msg.Span)!;
                    reply = JsonSerializer.SerializeToUtf8Bytes(m);
                }
                else
                {
                    var m = MemoryPackSerializer.Deserialize<MemoryPackMsg>(msg.Span)!;
                    reply = MemoryPackSerializer.Serialize(m);
                }

                while (!pub.TryEnqueue(reply)) Thread.SpinWait(50);
            }
        }) { IsBackground = true };
        t.Start();
        return t;
    }

    [Benchmark]
    public long Cloudtoid_MemoryPack()
    {
        var bytes = MemoryPackSerializer.Serialize(new MemoryPackMsg { Id = Guid.Empty, Kind = 1, Seq = 1, Payload = _payload });
        while (!_ctReqPub.TryEnqueue(bytes)) Thread.SpinWait(50);
        var resp = _ctRespSub.Dequeue(default);
        var back = MemoryPackSerializer.Deserialize<MemoryPackMsg>(resp.Span)!;
        return back.Payload.Length;
    }

    [Benchmark]
    public long Cloudtoid_Json()
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new JsonMsg { Id = Guid.Empty, Kind = 1, Seq = 1, Payload = _payload });
        while (!_ctJsonReqPub.TryEnqueue(bytes)) Thread.SpinWait(50);
        var resp = _ctJsonRespSub.Dequeue(default);
        var back = JsonSerializer.Deserialize<JsonMsg>(resp.Span)!;
        return back.Payload.Length;
    }

    // ============================ Named pipes ============================

    private void SetupPipes()
    {
        _pipeCts = new CancellationTokenSource();
        _pipeMpName = "sluice-bench-mp-" + Guid.NewGuid().ToString("N");
        _pipeJsonName = "sluice-bench-json-" + Guid.NewGuid().ToString("N");

        StartPipeEcho(_pipeMpName, _pipeCts.Token, json: false);
        StartPipeEcho(_pipeJsonName, _pipeCts.Token, json: true);

        _pipeMpClient = new NamedPipeClientStream(".", _pipeMpName, PipeDirection.InOut);
        _pipeMpClient.Connect(5000);
        _pipeJsonClient = new NamedPipeClientStream(".", _pipeJsonName, PipeDirection.InOut);
        _pipeJsonClient.Connect(5000);
    }

    private void StartPipeEcho(string name, CancellationToken ct, bool json)
    {
        new Thread(() =>
        {
            using var server = new NamedPipeServerStream(name, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.None);
            try { server.WaitForConnection(); }
            catch { return; }
            var lenBuf = new byte[4];
            while (!ct.IsCancellationRequested)
            {
                if (!ReadExact(server, lenBuf, 4)) break;
                int len = BitConverter.ToInt32(lenBuf);
                var body = new byte[len];
                if (!ReadExact(server, body, len)) break;

                byte[] reply;
                if (json)
                {
                    var m = JsonSerializer.Deserialize<JsonMsg>(body)!;
                    reply = JsonSerializer.SerializeToUtf8Bytes(m);
                }
                else
                {
                    var m = MemoryPackSerializer.Deserialize<MemoryPackMsg>(body)!;
                    reply = MemoryPackSerializer.Serialize(m);
                }

                server.Write(BitConverter.GetBytes(reply.Length));
                server.Write(reply);
                server.Flush();
            }
        }) { IsBackground = true }.Start();
    }

    [Benchmark]
    public int NamedPipe_MemoryPack()
    {
        var bytes = MemoryPackSerializer.Serialize(new MemoryPackMsg { Id = Guid.Empty, Kind = 1, Seq = 1, Payload = _payload });
        _pipeMpClient.Write(BitConverter.GetBytes(bytes.Length));
        _pipeMpClient.Write(bytes);
        _pipeMpClient.Flush();
        ReadExact(_pipeMpClient, _lenBuf, 4);
        int len = BitConverter.ToInt32(_lenBuf);
        var body = new byte[len];
        ReadExact(_pipeMpClient, body, len);
        return MemoryPackSerializer.Deserialize<MemoryPackMsg>(body)!.Payload.Length;
    }

    [Benchmark]
    public int NamedPipe_Json()
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new JsonMsg { Id = Guid.Empty, Kind = 1, Seq = 1, Payload = _payload });
        _pipeJsonClient.Write(BitConverter.GetBytes(bytes.Length));
        _pipeJsonClient.Write(bytes);
        _pipeJsonClient.Flush();
        ReadExact(_pipeJsonClient, _lenBuf, 4);
        int len = BitConverter.ToInt32(_lenBuf);
        var body = new byte[len];
        ReadExact(_pipeJsonClient, body, len);
        return JsonSerializer.Deserialize<JsonMsg>(body)!.Payload.Length;
    }

    private static bool ReadExact(Stream s, byte[] buf, int count)
    {
        int read = 0;
        while (read < count)
        {
            int n = s.Read(buf, read, count - read);
            if (n == 0) return false;
            read += n;
        }
        return true;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sluiceClient?.Dispose();
        _sluiceServer?.Dispose();

        _ctCts?.Cancel();
        _pipeCts?.Cancel();
        try { _pipeMpClient?.Dispose(); } catch { }
        try { _pipeJsonClient?.Dispose(); } catch { }
    }
}
