using System.Diagnostics;
using Sluice;

namespace Sluice.Benchmarks;

/// <summary>
/// A parallel aggregate-throughput measurement (messages/second) for the multicast ring under N producers
/// and M broadcast consumers — the "fastest zero-friction throughput" headline. Run with
/// <c>dotnet run -c Release -- throughput</c>.
///
/// In lossy mode producers never block on consumers, so the reported figure is sustained publish throughput;
/// consumers run concurrently to show the fan-out is real, and their seen/dropped counts are printed.
/// </summary>
public static class ThroughputHarness
{
    public static void Run()
    {
        Console.WriteLine("Sluice multicast throughput — lossy broadcast, 64-byte messages\n");
        Console.WriteLine($"{"producers",-10}{"consumers",-10}{"messages",-14}{"msgs/sec",-16}{"GB/sec",-9}");
        Console.WriteLine(new string('-', 59));
        foreach (var (producers, consumers) in new[] { (1, 0), (1, 1), (1, 4), (2, 2), (4, 4), (4, 1) })
            Measure(producers, consumers, perProducer: 2_000_000, payload: 64);
    }

    private static void Measure(int producers, int consumers, int perProducer, int payload)
    {
        using var ring = ShmMulticast.Create("tp-" + Guid.NewGuid().ToString("N"),
            maxPayload: payload, slotCount: 1 << 16, mode: DeliveryMode.Lossy, maxConsumers: 16);

        long total = (long)producers * perProducer;
        var subs = Enumerable.Range(0, consumers).Select(_ => ring.Subscribe()).ToArray();
        using var stop = new CancellationTokenSource();

        var consumerTasks = subs.Select(s => Task.Run(() =>
        {
            long seen = 0;
            var spin = new SpinWait();
            while (!stop.IsCancellationRequested)
            {
                if (s.TryRead(out _)) { s.Advance(); seen++; }
                else spin.SpinOnce();
            }
            return seen;
        })).ToArray();

        var msg = new byte[payload];
        var sw = Stopwatch.StartNew();
        Parallel.For(0, producers, _ =>
        {
            for (int i = 0; i < perProducer; i++) ring.Publish(msg);
        });
        sw.Stop();                       // producer throughput is measured here (lossy = no consumer gating)
        stop.Cancel();
        Task.WaitAll(consumerTasks);

        double secs = sw.Elapsed.TotalSeconds;
        double mps = total / secs;
        double gbps = mps * (payload + 12) / 1e9;
        Console.WriteLine($"{producers,-10}{consumers,-10}{total,-14:N0}{mps,-16:N0}{gbps,-9:F2}");

        foreach (var s in subs) s.Dispose();
    }
}
