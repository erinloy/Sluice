using BenchmarkDotNet.Attributes;
using Sluice;

namespace Sluice.Benchmarks;

/// <summary>
/// Per-message cost of the multi-way multicast ring: a raw publish, and a publish + in-place consume. These
/// are the numbers that set the throughput ceiling (msgs/sec ≈ 1 / time-per-message). A parallel aggregate
/// run is available via <c>dotnet run -c Release -- throughput</c>.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MulticastBenchmarks
{
    private ShmMulticast _lossy = null!;
    private ShmMulticast _withConsumer = null!;
    private ShmMulticast.Subscriber _sub = null!;
    private byte[] _payload = null!;

    [Params(32, 256)]
    public int PayloadSize;

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadSize];
        new Random(7).NextBytes(_payload);

        _lossy = ShmMulticast.Create("bench-mc-lossy-" + Guid.NewGuid().ToString("N"),
            maxPayload: 256, slotCount: 4096, mode: DeliveryMode.Lossy);

        _withConsumer = ShmMulticast.Create("bench-mc-rt-" + Guid.NewGuid().ToString("N"),
            maxPayload: 256, slotCount: 4096, mode: DeliveryMode.Lossy);
        _sub = _withConsumer.Subscribe();
    }

    /// <summary>Raw publish into a lossy ring with no consumer gating — the fan-out write cost.</summary>
    [Benchmark]
    public long Publish() => _lossy.Publish(_payload);

    /// <summary>Publish then consume one message in place (read straight from shared memory, no copy).</summary>
    [Benchmark]
    public int PublishAndConsumeInPlace()
    {
        _withConsumer.Publish(_payload);
        _sub.TryRead(out var span);
        int len = span.Length;
        _sub.Advance();
        return len;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sub.Dispose();
        _lossy.Dispose();
        _withConsumer.Dispose();
    }
}
