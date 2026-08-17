using System.Text;
using Sluice;
using Xunit;

namespace Sluice.Tests;

public class FrameChannelTests
{
    private static string N() => "fc." + Guid.NewGuid().ToString("N");

    [Fact]
    public void Server_and_client_exchange_frames_both_directions_in_place()
    {
        var name = N();
        using var server = ShmFrameChannel.CreateServerSide(name);
        using var client = ShmFrameChannel.ConnectClientSide(name);

        // client → server
        client.WriteFrame(Encoding.UTF8.GetBytes("request-1"));
        Assert.True(server.WaitForFrame(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token));
        Assert.True(server.TryReadFrame(out var inbound));
        Assert.Equal("request-1", Encoding.UTF8.GetString(inbound));
        server.AdvanceFrame();

        // server → client (full duplex)
        server.WriteFrame(Encoding.UTF8.GetBytes("response-1"));
        Assert.True(client.WaitForFrame(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token));
        Assert.True(client.TryReadFrame(out var reply));
        Assert.Equal("response-1", Encoding.UTF8.GetString(reply));
        client.AdvanceFrame();
    }

    [Fact]
    public async Task Listener_multiplexes_many_clients_each_on_its_own_duplex_channel()
    {
        var endpoint = "lsn-" + Guid.NewGuid().ToString("N");
        using var listener = new ShmFrameListener(endpoint);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        // The daemon: accept connections and echo each frame back, prefixed — proving per-connection routing.
        var serverTask = Task.Run(() =>
        {
            const int clients = 5;
            var workers = new List<Task>();
            for (int i = 0; i < clients; i++)
            {
                var ch = listener.Accept(cts.Token);
                workers.Add(Task.Run(() =>
                {
                    while (ch.WaitForFrame(cts.Token))
                    {
                        if (!ch.TryReadFrame(out var f)) continue;
                        var reply = Encoding.UTF8.GetBytes("echo:" + Encoding.UTF8.GetString(f));
                        ch.AdvanceFrame();          // release before writing the reply
                        if (reply.Length == "echo:STOP".Length && Encoding.UTF8.GetString(reply) == "echo:STOP") break;
                        ch.WriteFrame(reply);
                    }
                    ch.Dispose();
                }));
            }
            Task.WaitAll(workers.ToArray());
        });

        // Five independent client "processes", each its own connection.
        Parallel.For(0, 5, c =>
        {
            using var ch = ShmFrameChannel.Connect(endpoint);
            for (int i = 0; i < 50; i++)
            {
                var msg = $"c{c}-{i}";
                ch.WriteFrame(Encoding.UTF8.GetBytes(msg));
                Assert.True(ch.WaitForFrame(cts.Token));
                Assert.True(ch.TryReadFrame(out var reply));
                Assert.Equal("echo:" + msg, Encoding.UTF8.GetString(reply));
                ch.AdvanceFrame();
            }
            ch.WriteFrame(Encoding.UTF8.GetBytes("STOP"));
        });

        await serverTask.WaitAsync(TimeSpan.FromSeconds(15));
    }

    [Fact]
    public async Task Frames_preserve_order_and_boundaries_under_load()
    {
        var name = N();
        using var server = ShmFrameChannel.CreateServerSide(name);
        using var client = ShmFrameChannel.ConnectClientSide(name);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        const int n = 20_000;
        var reader = Task.Run(() =>
        {
            int got = 0;
            while (got < n && server.WaitForFrame(cts.Token))
                while (server.TryReadFrame(out var f))
                {
                    Assert.Equal($"msg-{got}", Encoding.UTF8.GetString(f)); // exact boundary + order
                    got++;
                    server.AdvanceFrame();
                }
            return got;
        });

        for (int i = 0; i < n; i++)
            client.WriteFrame(Encoding.UTF8.GetBytes($"msg-{i}"), cts.Token);

        Assert.Equal(n, await reader);
    }
}
