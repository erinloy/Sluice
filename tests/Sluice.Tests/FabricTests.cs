using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Sluice.Fabric;
using Xunit;

namespace Sluice.Tests;

public class FabricTests
{
    private static string Ch() => "ch" + Guid.NewGuid().ToString("N");

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
    public void Local_channel_delivers_broadcast_and_addressed_send_through_the_seam()
    {
        var name = Ch();
        using var ta = new ShmTransport("a");
        using var tb = new ShmTransport("b");
        using var ca = ta.Open(name);
        using var cb = tb.Open(name);

        var inbox = new ConcurrentQueue<(int kind, ParticipantId from, string text)>();
        using var sub = cb.Subscribe((in Inbound m) =>
            inbox.Enqueue((m.Kind, m.From, Encoding.UTF8.GetString(m.Payload))));

        // 'a' broadcasts to the channel, then sends one addressed straight to 'b'.
        ca.Broadcast(7, Encoding.UTF8.GetBytes("hello-all"));
        ca.Send(new ParticipantId($"fabric.{name}.b"), 9, Encoding.UTF8.GetBytes("for-b-only"));

        Assert.True(SpinUntil(() => inbox.Count >= 2), $"expected 2 messages, saw {inbox.Count}");
        Assert.Contains(inbox, x => x.kind == 7 && x.text == "hello-all");
        Assert.Contains(inbox, x => x.kind == 9 && x.text == "for-b-only");
        // The sender's identity survives the seam (so replies can be addressed back).
        Assert.All(inbox, x => Assert.Equal($"fabric.{name}.a", x.from.Value));
    }

    [Fact]
    public void Federated_transport_with_only_a_local_member_behaves_like_the_local_transport()
    {
        // With no remote member supplied, federation must not cost the local fast path — a sanity check that the
        // composing router degrades cleanly to its local member.
        var name = Ch();
        using var fed = new FederatedTransport(new ShmTransport("a"));
        using var local = new ShmTransport("b");
        using var cf = fed.Open(name);
        using var cl = local.Open(name);

        var inbox = new ConcurrentQueue<string>();
        using var sub = cl.Subscribe((in Inbound m) => inbox.Enqueue(Encoding.UTF8.GetString(m.Payload)));

        cf.Broadcast(1, Encoding.UTF8.GetBytes("via-federation"));

        Assert.True(SpinUntil(() => inbox.Count >= 1), "federated broadcast was not delivered locally");
        Assert.Equal("via-federation", inbox.First());
        Assert.False(fed.Owns(new ParticipantId("node:remote-host/x")), "a local-only fabric must not claim a remote id");
    }

    [Fact]
    public void Tcp_transport_delivers_broadcast_and_addressed_across_a_socket()
    {
        int portA = FreePort(), portB = FreePort();
        var idA = new ParticipantId("node:alice");
        var idB = new ParticipantId("node:bob");
        // Only A dials B (single connection, no duplicate from mutual dialing). Both are addressable over it.
        using var a = new TcpTransport(idA, portA, (idB, "127.0.0.1", portB));
        using var b = new TcpTransport(idB, portB);

        var name = Ch();
        using var ca = a.Open(name);
        using var cb = b.Open(name);

        var inbox = new ConcurrentQueue<(int kind, string from, string text)>();
        using var sub = cb.Subscribe((in Inbound m) =>
            inbox.Enqueue((m.Kind, m.From.Value, Encoding.UTF8.GetString(m.Payload))));

        Assert.True(SpinUntil(() => ca.Participants.Any(p => p.Value == idB.Value)),
            "TCP hello handshake did not register the peer");

        ca.Broadcast(7, Encoding.UTF8.GetBytes("hello-net"));
        ca.Send(idB, 9, Encoding.UTF8.GetBytes("for-bob"));

        Assert.True(SpinUntil(() => inbox.Count >= 2), $"expected 2 over the socket, saw {inbox.Count}");
        Assert.Contains(inbox, x => x.kind == 7 && x.text == "hello-net" && x.from == "node:alice");
        Assert.Contains(inbox, x => x.kind == 9 && x.text == "for-bob");
    }

    [Fact]
    public void Federated_channel_spans_a_local_shm_participant_and_a_remote_tcp_participant()
    {
        // The whole point of the fabric: one Broadcast reaches a co-located shared-memory participant AND a
        // participant on the other end of a socket, from the same call.
        int portA = FreePort(), portB = FreePort();
        var name = Ch();

        using var fed = new FederatedTransport(
            new ShmTransport("alice"),
            new TcpTransport(new ParticipantId("node:alice"), portA, (new ParticipantId("node:bob"), "127.0.0.1", portB)));
        using var remote = new TcpTransport(new ParticipantId("node:bob"), portB);
        using var localShm = new ShmTransport("carol");   // a co-located participant on node A's host

        using var cf = fed.Open(name);
        using var cRemote = remote.Open(name);
        using var cLocal = localShm.Open(name);

        var remoteInbox = new ConcurrentQueue<string>();
        using var subR = cRemote.Subscribe((in Inbound m) => remoteInbox.Enqueue(Encoding.UTF8.GetString(m.Payload)));
        var localInbox = new ConcurrentQueue<string>();
        using var subL = cLocal.Subscribe((in Inbound m) => localInbox.Enqueue(Encoding.UTF8.GetString(m.Payload)));

        Assert.True(SpinUntil(() => cf.Participants.Any(p => p.Value == "node:bob")),
            "federated transport never connected to the remote peer");

        cf.Broadcast(1, Encoding.UTF8.GetBytes("everywhere"));

        Assert.True(SpinUntil(() => remoteInbox.Count >= 1), "broadcast did not reach the remote TCP participant");
        Assert.True(SpinUntil(() => localInbox.Count >= 1), "broadcast did not reach the local shm participant");
        Assert.Equal("everywhere", remoteInbox.First());
        Assert.Equal("everywhere", localInbox.First());
    }
}
