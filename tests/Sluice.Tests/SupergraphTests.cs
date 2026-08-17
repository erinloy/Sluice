using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Sluice.Fabric;
using Sluice.Fusion;
using Sluice.Supergraph;
using Xunit;

namespace Sluice.Tests;

public class SupergraphTests
{
    private static string Graph() => "g" + Guid.NewGuid().ToString("N");

    private static readonly SluiceCodec<int> Int = new(
        i => { var b = new byte[4]; BinaryPrimitives.WriteInt32LittleEndian(b, i); return b; },
        span => BinaryPrimitives.ReadInt32LittleEndian(span));

    private static bool SpinUntil(Func<bool> cond, int seconds = 5)
    {
        var until = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < until) { if (cond()) return true; Thread.Sleep(5); }
        return cond();
    }

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    [Fact]
    public void A_vertex_you_own_reads_locally_with_no_round_trip()
    {
        using var t = new ShmTransport("solo");
        using var g = new GraphPeer(t, Graph());

        var x = g.Define("x", Int);
        x.Set(99);

        var c = g.Get(x.Id, Int);
        Assert.True(c.Exists);
        Assert.Equal(99, c.Value);
        Assert.Equal(Reach.Local, c.Reach);     // owned → a direct local read, not a fetch
        Assert.Equal(g.Self, x.Id.Owner);
    }

    [Fact]
    public void A_participant_observes_and_reacts_to_another_participants_vertex()
    {
        var graph = Graph();
        using var tOwner = new ShmTransport("owner");
        using var tWatcher = new ShmTransport("watcher");
        using var owner = new GraphPeer(tOwner, graph);
        using var watcher = new GraphPeer(tWatcher, graph);

        var x = owner.Define("x", Int);
        using var view = watcher.Observe(x.Id, Int);   // x.Id.Owner == owner.Self

        x.Set(7);
        Assert.True(SpinUntil(() => view.Exists && view.Current == 7),
            $"watcher never saw 7 (saw {view.Current}, exists={view.Exists})");

        x.Set(8);
        Assert.True(SpinUntil(() => view.Current == 8), $"watcher never saw the update to 8 (saw {view.Current})");
    }

    [Fact]
    public void A_computed_vertex_propagates_a_change_across_three_participants()
    {
        // A owns x; B computes y = 2*x (a remote dependency on A); C observes y. A change to x on A must ripple
        // A → B → C with no direct A↔C link — the supergraph reacting as one graph.
        var graph = Graph();
        using var tA = new ShmTransport("A");
        using var tB = new ShmTransport("B");
        using var tC = new ShmTransport("C");
        using var A = new GraphPeer(tA, graph);
        using var B = new GraphPeer(tB, graph);
        using var C = new GraphPeer(tC, graph);

        var x = A.Define("x", Int);
        using var y = B.Computed("y", Int, () => B.Get(x.Id, Int).Value * 2, x.Id);
        using var view = C.Observe(y.Id, Int);

        x.Set(10);
        Assert.True(SpinUntil(() => view.Exists && view.Current == 20, 10),
            $"C never saw y=20 after x=10 (saw {view.Current})");

        x.Set(50);
        Assert.True(SpinUntil(() => view.Current == 100, 10),
            $"the change to x=50 did not propagate A→B→C (C saw {view.Current})");
    }

    [Fact]
    public void The_three_hop_graph_converges_on_every_repetition()
    {
        // Robustness guard: the A→B→C reactive chain must converge every time, even when the background pump and
        // observer threads are scheduled late under load — invalidation delivery is reliable, so a generous budget
        // must always settle. (This is the regression test for the ShmPeer concurrent-send + pump-survives-throw
        // hardening; before those, this chain could silently wedge.)
        for (int iter = 0; iter < 25; iter++)
        {
            var graph = Graph();
            using var tA = new ShmTransport("A");
            using var tB = new ShmTransport("B");
            using var tC = new ShmTransport("C");
            using var A = new GraphPeer(tA, graph);
            using var B = new GraphPeer(tB, graph);
            using var C = new GraphPeer(tC, graph);

            var x = A.Define("x", Int);
            using var y = B.Computed("y", Int, () => B.Get(x.Id, Int).Value * 2, x.Id);
            using var view = C.Observe(y.Id, Int);

            x.Set(iter + 1);
            int expected = (iter + 1) * 2;
            Assert.True(SpinUntil(() => view.Current == expected, 8),
                $"iter {iter}: chain did not converge to {expected} (C saw {view.Current})");
        }
    }

    [Fact]
    public void A_federated_supergraph_reacts_across_a_socket()
    {
        // The headline: the owner is a participant on host A (reached over shared memory at home, over TCP from
        // afar); the observer is on host B. One Set on A propagates across the socket to B's reactive view.
        int portA = FreePort(), portB = FreePort();
        var graph = Graph();

        using var fedA = new FederatedTransport(
            new ShmTransport("alice"),
            new TcpTransport(new ParticipantId("node:alice"), portA, (new ParticipantId("node:bob"), "127.0.0.1", portB)));
        using var fedB = new FederatedTransport(
            new ShmTransport("bob"),
            new TcpTransport(new ParticipantId("node:bob"), portB));

        using var A = new GraphPeer(fedA, graph);
        using var B = new GraphPeer(fedB, graph);

        var x = A.Define("x", Int);

        // Wait for the TCP handshake so B can address A.
        Assert.True(SpinUntil(() => A.Participants.Any(p => p.Value == "node:bob")),
            "federated transports never connected over TCP");

        // B reaches A by its network id — the reach-relative owner address.
        using var view = B.Observe(new VertexId(new ParticipantId("node:alice"), "x"), Int);

        x.Set(7);
        Assert.True(SpinUntil(() => view.Exists && view.Current == 7, 10),
            $"the change did not cross the socket (B saw {view.Current}, exists={view.Exists})");
        Assert.Equal(Reach.Remote, view.Reach);   // B read it over the network

        x.Set(7777);
        Assert.True(SpinUntil(() => view.Current == 7777, 10),
            $"the second update did not cross the socket (B saw {view.Current})");
    }
}
