using System.Text;
using Sluice;
using Sluice.Multiway;
using Xunit;

namespace Sluice.Tests;

public class MultiwayApiTests
{
    private static string N() => Guid.NewGuid().ToString("N");

    [Fact]
    public void Topic_broadcasts_to_every_subscriber()
    {
        using var topic = new ShmTopic(N(), mode: DeliveryMode.Reliable);
        var s1 = topic.Subscribe();
        var s2 = topic.Subscribe();

        for (int i = 0; i < 100; i++) topic.Publish(BitConverter.GetBytes(i));

        foreach (var s in new[] { s1, s2 })
        {
            int count = 0;
            while (s.TryRead(out var span)) { Assert.Equal(count, BitConverter.ToInt32(span)); count++; s.Advance(); }
            Assert.Equal(100, count);
            s.Dispose();
        }
    }

    [Fact]
    public void Topic_is_a_many_to_many_bus_across_independent_handles()
    {
        var name = N();
        // Two independent handles to the same topic = two "processes": first creates, second attaches.
        using var p1 = new ShmTopic(name, mode: DeliveryMode.Reliable, slotCount: 2048);
        using var p2 = new ShmTopic(name, mode: DeliveryMode.Reliable, slotCount: 2048);

        var sub = p1.Subscribe();
        for (int i = 0; i < 200; i++) p1.Publish(Encoding.UTF8.GetBytes($"p1-{i}"));
        for (int i = 0; i < 200; i++) p2.Publish(Encoding.UTF8.GetBytes($"p2-{i}"));

        int fromP1 = 0, fromP2 = 0;
        while (sub.TryRead(out var span))
        {
            var s = Encoding.UTF8.GetString(span);
            if (s.StartsWith("p1-")) fromP1++; else if (s.StartsWith("p2-")) fromP2++;
            sub.Advance();
        }
        Assert.Equal(200, fromP1);
        Assert.Equal(200, fromP2);
        sub.Dispose();
    }

    [Fact]
    public void Peers_communicate_full_duplex()
    {
        var a = "A-" + N();
        var b = "B-" + N();
        using var peerA = new ShmPeer(a);
        using var peerB = new ShmPeer(b);

        peerA.Send(b, Encoding.UTF8.GetBytes("hi B"));
        Assert.True(peerB.Receive(out var got, Cancel(5)));
        Assert.Equal("hi B", Encoding.UTF8.GetString(got));

        // B replies to A on the same mesh — no client/server roles.
        peerB.Send(a, Encoding.UTF8.GetBytes("hi back A"));
        Assert.True(peerA.Receive(out var reply, Cancel(5)));
        Assert.Equal("hi back A", Encoding.UTF8.GetString(reply));
    }

    [Fact]
    public void Multiple_peers_send_to_one_inbox_concurrently()
    {
        var target = "T-" + N();
        using var t = new ShmPeer(target, slotCount: 4096);
        var senders = Enumerable.Range(0, 6).Select(i => new ShmPeer($"S{i}-" + N())).ToArray();

        const int each = 200;
        Parallel.ForEach(senders, s =>
        {
            for (int i = 0; i < each; i++)
                s.Send(target, BitConverter.GetBytes(i));
        });

        int received = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (received < senders.Length * each && t.Receive(out _, cts.Token))
            received++;

        Assert.Equal(senders.Length * each, received);
        foreach (var s in senders) s.Dispose();
    }

    [Fact]
    public void Peer_presence_is_discoverable()
    {
        var name = "P-" + N();
        Assert.False(ShmPeer.IsPeerAlive(name, TimeSpan.FromSeconds(5)));
        using var peer = new ShmPeer(name);
        Assert.True(ShmPeer.IsPeerAlive(name, TimeSpan.FromSeconds(5)));
    }

    private static CancellationToken Cancel(int seconds) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;
}
