using System.Text;
using Sluice.Rpc;
using Xunit;

namespace Sluice.Tests;

public class RpcTests
{
    private static string Endpoint() => "rpc-" + Guid.NewGuid().ToString("N");

    private const int Echo = 1;
    private const int Upper = 2;
    private const int Count = 3;

    private static SluiceServer StartServer(string endpoint)
    {
        var server = new SluiceServer(endpoint, static (in RpcContext ctx) =>
        {
            switch (ctx.Kind)
            {
                case Echo:
                    ctx.Reply(ctx.Request);
                    break;
                case Upper:
                    var s = Encoding.UTF8.GetString(ctx.Request).ToUpperInvariant();
                    ctx.Reply(Encoding.UTF8.GetBytes(s));
                    break;
                case Count:
                    int n = ctx.Request.Length >= 4 ? BitConverter.ToInt32(ctx.Request) : 0;
                    for (int i = 0; i < n; i++)
                        ctx.StreamItem(BitConverter.GetBytes(i));
                    ctx.Complete();
                    break;
                default:
                    ctx.Reply(Encoding.UTF8.GetBytes("unknown"), ok: false);
                    break;
            }
        });
        server.Start();
        return server;
    }

    [Fact]
    public void Unary_request_echoes_in_place()
    {
        var endpoint = Endpoint();
        using var server = StartServer(endpoint);
        Thread.Sleep(100);

        using var client = new SluiceClient(endpoint);
        var resp = client.Send(Echo, Encoding.UTF8.GetBytes("ping"));

        Assert.True(resp.Ok);
        Assert.Equal("ping", resp.Text);
    }

    [Fact]
    public void Unary_handler_transforms_payload()
    {
        var endpoint = Endpoint();
        using var server = StartServer(endpoint);
        Thread.Sleep(100);

        using var client = new SluiceClient(endpoint);
        var resp = client.Send(Upper, Encoding.UTF8.GetBytes("hello"));

        Assert.Equal("HELLO", resp.Text);
    }

    [Fact]
    public void Many_sequential_requests_all_correlate()
    {
        var endpoint = Endpoint();
        using var server = StartServer(endpoint);
        Thread.Sleep(100);

        using var client = new SluiceClient(endpoint);
        for (int i = 0; i < 2000; i++)
        {
            var resp = client.Send(Echo, Encoding.UTF8.GetBytes($"n={i}"));
            Assert.Equal($"n={i}", resp.Text);
        }
    }

    [Fact]
    public void Zero_alloc_receive_reads_response_in_place()
    {
        var endpoint = Endpoint();
        using var server = StartServer(endpoint);
        Thread.Sleep(100);

        using var client = new SluiceClient(endpoint, exclusiveProducer: true);

        // The reader sees the response while it is still resident in the shared ring; copy out to assert.
        string? seen = null;
        bool seenOk = false;
        var reader = (SluiceClient.ResponseReader<object?>)((ok, resp, _) =>
        {
            seenOk = ok;
            seen = Encoding.UTF8.GetString(resp);
        });

        client.Send(Upper, Encoding.UTF8.GetBytes("zero"), (object?)null, reader);
        Assert.True(seenOk);
        Assert.Equal("ZERO", seen);

        // Correlation must still hold across many in-place round-trips.
        for (int i = 0; i < 1000; i++)
        {
            int expected = -1, got = -2;
            expected = i;
            client.Send(Echo, BitConverter.GetBytes(i), 0, (ok, resp, _) => got = BitConverter.ToInt32(resp));
            Assert.Equal(expected, got);
        }
    }

    [Fact]
    public void Streaming_response_yields_all_items_then_completes()
    {
        var endpoint = Endpoint();
        using var server = StartServer(endpoint);
        Thread.Sleep(100);

        using var client = new SluiceClient(endpoint);
        var items = client.SendStream(Count, BitConverter.GetBytes(50)).ToList();

        Assert.Equal(50, items.Count);
        for (int i = 0; i < 50; i++)
            Assert.Equal(i, BitConverter.ToInt32(items[i]));
    }

    [Fact]
    public void Multiple_client_processes_share_one_request_ring()
    {
        var endpoint = Endpoint();
        using var server = StartServer(endpoint);
        Thread.Sleep(100);

        // Distinct clients (each with its own response ring) hammering the shared MPSC request ring.
        Parallel.For(0, 8, c =>
        {
            using var client = new SluiceClient(endpoint);
            for (int i = 0; i < 250; i++)
            {
                var msg = $"c{c}-i{i}";
                var resp = client.Send(Echo, Encoding.UTF8.GetBytes(msg));
                Assert.Equal(msg, resp.Text);
            }
        });
    }
}
