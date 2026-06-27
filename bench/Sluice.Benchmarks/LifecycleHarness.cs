using System.Diagnostics;
using Sluice.Fusion;
using Sluice.Gossip;
using Sluice.Rpc;

namespace Sluice.Benchmarks;

/// <summary>
/// Times how long each Sluice subsystem takes to walk its lifecycle states — init → bootstrapped →
/// discovered → active → converged. Run with <c>dotnet run -c Release -- lifecycle</c>. Figures are
/// wall-clock milliseconds, median of several runs.
/// </summary>
public static class LifecycleHarness
{
    public static void Run()
    {
        Console.WriteLine("Sluice lifecycle timings (ms, median of 5 runs)\n");

        Rpc();
        Console.WriteLine();
        Fusion();
        Console.WriteLine();
        Gossip();
    }

    private static double Median(IEnumerable<double> xs)
    {
        var a = xs.OrderBy(x => x).ToArray();
        return a.Length == 0 ? 0 : a[a.Length / 2];
    }

    // Yielding wait so the background listener/gossip threads get CPU; bounded so a missed signal can't hang.
    private static void SpinUntil(Func<bool> cond, int timeoutMs = 10_000)
    {
        var sw = Stopwatch.StartNew();
        while (!cond())
        {
            if (sw.ElapsedMilliseconds > timeoutMs) return;
            Thread.Yield();
        }
    }

    private static void Row(string label, double ms) => Console.WriteLine($"  {label,-34}{ms,8:F3} ms");

    // ---- RPC daemon + thin client ----
    private static void Rpc()
    {
        Console.WriteLine("RPC (daemon + thin client)");
        var boot = new List<double>(); var disc = new List<double>(); var active = new List<double>();
        for (int i = 0; i < 5; i++)
        {
            var ep = "lc-rpc-" + Guid.NewGuid().ToString("N");
            var sw = Stopwatch.StartNew();
            using var owner = SluiceDiscovery.TryBecomeOwner(ep);
            using var server = new SluiceServer(ep, static (in RpcContext c) => c.Reply(c.Request));
            server.Start();
            SluiceDiscovery.Heartbeat(ep, 1 << 20);
            boot.Add(sw.Elapsed.TotalMilliseconds);                       // bootstrapped: server up + lease

            sw.Restart();
            SpinUntil(() => SluiceDiscovery.IsAlive(ep, TimeSpan.FromSeconds(5)));
            disc.Add(sw.Elapsed.TotalMilliseconds);                       // discovered: client finds the lease

            using var client = new SluiceClient(ep, exclusiveProducer: true);
            sw.Restart();
            client.Send(1, "ping"u8);
            active.Add(sw.Elapsed.TotalMilliseconds);                     // active: first round-trip completes
        }
        Row("init -> bootstrapped (daemon)", Median(boot));
        Row("bootstrapped -> discovered", Median(disc));
        Row("discovered -> active (1st call)", Median(active));
    }

    // ---- Fusion overlay host + mirror ----
    private static void Fusion()
    {
        Console.WriteLine("Fusion overlay (host + mirror)");
        var hostBoot = new List<double>(); var attach = new List<double>();
        var firstFetch = new List<double>(); var invalLatency = new List<double>();
        for (int i = 0; i < 5; i++)
        {
            var model = "lc-fusion-" + Guid.NewGuid().ToString("N");
            var sw = Stopwatch.StartNew();
            using var host = new ModelHost(model);
            hostBoot.Add(sw.Elapsed.TotalMilliseconds);                   // bootstrapped: topic + rpc up
            host.Set("k", "v1"u8.ToArray());

            sw.Restart();
            using var mirror = new ModelMirror(model);
            attach.Add(sw.Elapsed.TotalMilliseconds);                     // discovered: mirror attached

            sw.Restart();
            var c = mirror.Get("k");
            firstFetch.Add(sw.Elapsed.TotalMilliseconds);                 // active: first fetch round-trips

            sw.Restart();
            host.Set("k", "v2"u8.ToArray());
            SpinUntil(() => c.State == ConsistencyState.Invalidated);
            invalLatency.Add(sw.Elapsed.TotalMilliseconds);              // converged: invalidation propagates
        }
        Row("init -> bootstrapped (host)", Median(hostBoot));
        Row("mirror attach", Median(attach));
        Row("active (1st fetch)", Median(firstFetch));
        Row("invalidation propagation", Median(invalLatency));
    }

    // ---- Gossip cluster ----
    private static void Gossip()
    {
        Console.WriteLine("Gossip cluster");
        foreach (int n in new[] { 3, 5, 8 })
        {
            var disc = new List<double>(); var conv = new List<double>();
            for (int run = 0; run < 5; run++)
            {
                var cluster = "lc-g-" + Guid.NewGuid().ToString("N");
                var nodes = Enumerable.Range(0, n)
                    .Select(i => new GossipNode(cluster, "n" + i, gossipInterval: TimeSpan.FromMilliseconds(20)))
                    .ToArray();

                var sw = Stopwatch.StartNew();
                foreach (var node in nodes) node.Start();
                SpinUntil(() => nodes.All(x => x.IsDiscovered));
                disc.Add(sw.Elapsed.TotalMilliseconds);                   // discovered: every node heard a peer

                sw.Restart();
                for (int i = 0; i < n; i++) nodes[i].Set("k" + i, "v"u8.ToArray());
                SpinUntil(() => nodes.All(node => Enumerable.Range(0, n).All(i => node.TryGet("k" + i, out _))));
                conv.Add(sw.Elapsed.TotalMilliseconds);                   // converged: all nodes have all keys

                foreach (var node in nodes) node.Dispose();
            }
            Console.WriteLine($"  N={n}");
            Row("    init -> all discovered", Median(disc));
            Row("    seed -> fully converged", Median(conv));
        }
    }
}
