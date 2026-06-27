using System.Runtime.InteropServices;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using MemoryPack;
using MessagePack;

namespace Sluice.Benchmarks;

/// <summary>
/// Isolates the serialization axis: the round-trip cost of encoding then decoding one message. This is the
/// cost Sluice eliminates entirely on its hot path. "Blittable" measures the zero-serialization path — write
/// the header with MemoryMarshal and read it back in place — which is what living in shared memory buys you.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class SerializationBenchmarks
{
    [Params(64, 1024)]
    public int PayloadSize;

    private byte[] _payload = [];
    private MemoryPackMsg _mp = null!;
    private MessagePackMsg _msgpack = null!;
    private JsonMsg _json = null!;
    private byte[] _scratch = new byte[8192];

    private byte[] _mpBytes = [];
    private byte[] _msgpackBytes = [];
    private byte[] _jsonBytes = [];
    private int _blitLen;

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadSize];
        new Random(42).NextBytes(_payload);
        var id = Guid.NewGuid();

        _mp = new MemoryPackMsg { Id = id, Kind = 7, Seq = 12345, Payload = _payload };
        _msgpack = new MessagePackMsg { Id = id, Kind = 7, Seq = 12345, Payload = _payload };
        _json = new JsonMsg { Id = id, Kind = 7, Seq = 12345, Payload = _payload };

        // Pre-encode for the decode half of each round trip.
        _mpBytes = MemoryPackSerializer.Serialize(_mp);
        _msgpackBytes = MessagePackSerializer.Serialize(_msgpack);
        _jsonBytes = JsonSerializer.SerializeToUtf8Bytes(_json);

        var header = new BlittableHeader(id, 7, 12345, PayloadSize);
        MemoryMarshal.Write(_scratch, in header);
        _payload.CopyTo(_scratch.AsSpan(32));
        _blitLen = 32 + PayloadSize;
    }

    [Benchmark(Baseline = true)]
    public long Json()
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(_json);
        var back = JsonSerializer.Deserialize<JsonMsg>(_jsonBytes)!;
        return bytes.Length + back.Seq;
    }

    [Benchmark]
    public long MessagePack_()
    {
        var bytes = MessagePackSerializer.Serialize(_msgpack);
        var back = MessagePackSerializer.Deserialize<MessagePackMsg>(_msgpackBytes);
        return bytes.Length + back.Seq;
    }

    [Benchmark]
    public long MemoryPack_()
    {
        var bytes = MemoryPackSerializer.Serialize(_mp);
        var back = MemoryPackSerializer.Deserialize<MemoryPackMsg>(_mpBytes)!;
        return bytes.Length + back.Seq;
    }

    [Benchmark]
    public long Blittable_InPlace()
    {
        // Encode: stamp the header + copy the payload into the buffer.
        var header = new BlittableHeader(_mp.Id, 7, 12345, PayloadSize);
        MemoryMarshal.Write(_scratch, in header);
        _payload.CopyTo(_scratch.AsSpan(32));

        // Decode: reinterpret the header in place + slice the payload — no allocation, no field decode.
        ref readonly BlittableHeader back = ref MemoryMarshal.AsRef<BlittableHeader>(_scratch);
        ReadOnlySpan<byte> payload = _scratch.AsSpan(32, back.PayloadLength);
        return _blitLen + back.Seq + payload.Length;
    }
}
