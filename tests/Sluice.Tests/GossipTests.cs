using System.Text;
using Sluice.Gossip;
using Xunit;

namespace Sluice.Tests;

public class GossipTests
{
    private static string Cluster() => "g-" + Guid.NewGuid().ToString("N");

    private static bool Eventually(Func<bool> cond, int timeoutMs = 5000)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (cond()) return true;
            Thread.Sleep(10);
        }
        return cond();
    }

    [Fact]
    public void Updates_converge_across_all_nodes()
    {
        var cluster = Cluster();
        using var a = new GossipNode(cluster, "a");
        using var b = new GossipNode(cluster, "b");
        using var c = new GossipNode(cluster, "c");
        a.Start(); b.Start(); c.Start();

        a.Set("x", "1"u8.ToArray());
        b.Set("y", "2"u8.ToArray());
        c.Set("z", "3"u8.ToArray());

        foreach (var node in new[] { a, b, c })
        {
            Assert.True(Eventually(() => node.TryGet("x", out var v) && v.AsSpan().SequenceEqual("1"u8)), $"{node.NodeId} missing x");
            Assert.True(Eventually(() => node.TryGet("y", out var v) && v.AsSpan().SequenceEqual("2"u8)), $"{node.NodeId} missing y");
            Assert.True(Eventually(() => node.TryGet("z", out var v) && v.AsSpan().SequenceEqual("3"u8)), $"{node.NodeId} missing z");
        }
    }

    [Fact]
    public void Last_writer_wins_by_version()
    {
        var cluster = Cluster();
        using var a = new GossipNode(cluster, "a");
        using var b = new GossipNode(cluster, "b");
        a.Start(); b.Start();

        a.Set("k", "old"u8.ToArray());
        Assert.True(Eventually(() => b.TryGet("k", out var v) && v.AsSpan().SequenceEqual("old"u8)));

        // b overwrites with a higher Lamport version (it has observed a's update, so its clock is ahead)
        b.Set("k", "new"u8.ToArray());
        Assert.True(Eventually(() => a.TryGet("k", out var v) && v.AsSpan().SequenceEqual("new"u8)));
        foreach (var node in new[] { a, b })
            Assert.True(Eventually(() => node.TryGet("k", out var v) && v.AsSpan().SequenceEqual("new"u8)));
    }

    [Fact]
    public void Nodes_discover_each_other()
    {
        var cluster = Cluster();
        using var a = new GossipNode(cluster, "a");
        using var b = new GossipNode(cluster, "b");
        a.Start(); b.Start();

        Assert.True(Eventually(() => a.LivePeerCount >= 1 && b.LivePeerCount >= 1));
        Assert.True(a.IsDiscovered);
        Assert.True(b.IsDiscovered);
    }

    [Fact]
    public void Late_joiner_catches_up_via_anti_entropy()
    {
        var cluster = Cluster();
        using var a = new GossipNode(cluster, "a", gossipInterval: TimeSpan.FromMilliseconds(20));
        a.Start();
        a.Set("seeded", "value"u8.ToArray());
        Thread.Sleep(50);

        // b joins AFTER the rumor was already broadcast — only anti-entropy reinforcement can deliver it.
        using var b = new GossipNode(cluster, "b", gossipInterval: TimeSpan.FromMilliseconds(20));
        b.Start();
        Assert.True(Eventually(() => b.TryGet("seeded", out var v) && v.AsSpan().SequenceEqual("value"u8)));
    }
}
