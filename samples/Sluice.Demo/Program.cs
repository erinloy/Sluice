using System.Collections.Concurrent;
using System.Text;
using Sluice.Rpc;

// A tiny end-to-end demo of the daemon + thin-CLI pattern Sluice is built for:
//
//   kvd serve              start the daemon (holds the key-value state in memory)
//   kvd set <key> <value>  a fresh short-lived process that DISCOVERS the daemon and talks to it
//   kvd get <key>
//   kvd status
//
// The point: `set`/`get`/`status` do NOT load the state — they reach into the already-running `serve`
// process over shared memory, which is the whole reason this is faster than re-spinning state per call.

const string Endpoint = "kvd";
const int OpStatus = 0, OpGet = 1, OpSet = 2;
var staleAfter = TimeSpan.FromSeconds(10);

if (args.Length == 0)
{
    Console.WriteLine("usage: kvd <serve|set|get|status> ...");
    return 1;
}

switch (args[0])
{
    case "serve":
        return Serve();
    case "set" when args.Length >= 3:
        return Set(args[1], args[2]);
    case "get" when args.Length >= 2:
        return Get(args[1]);
    case "status":
        return Status();
    default:
        Console.WriteLine("usage: kvd <serve|set|get|status> ...");
        return 1;
}

int Serve()
{
    using var owner = SluiceDiscovery.TryBecomeOwner(Endpoint);
    if (owner is null)
    {
        Console.Error.WriteLine("a kvd daemon is already running for this endpoint.");
        return 1;
    }

    var store = new ConcurrentDictionary<string, string>();

    using var server = new SluiceServer(Endpoint, (in RpcContext ctx) =>
    {
        switch (ctx.Kind)
        {
            case OpStatus:
                ctx.Reply(Encoding.UTF8.GetBytes($"alive; keys={store.Count}; pid={Environment.ProcessId}"));
                break;
            case OpGet:
            {
                var key = Encoding.UTF8.GetString(ctx.Request);
                ctx.Reply(store.TryGetValue(key, out var v)
                    ? Encoding.UTF8.GetBytes(v)
                    : Encoding.UTF8.GetBytes("(not found)"), ok: store.ContainsKey(key));
                break;
            }
            case OpSet:
            {
                // payload = key \0 value
                var text = Encoding.UTF8.GetString(ctx.Request);
                int nul = text.IndexOf('\0');
                store[text[..nul]] = text[(nul + 1)..];
                ctx.Reply(Encoding.UTF8.GetBytes("ok"));
                break;
            }
        }
    });
    server.Start();
    SluiceDiscovery.Heartbeat(Endpoint, 1 << 20);

    using var beat = new Timer(_ => SluiceDiscovery.Heartbeat(Endpoint, 1 << 20), null, 2000, 2000);
    Console.WriteLine($"kvd serving endpoint '{Endpoint}' (pid {Environment.ProcessId}). Ctrl+C to stop.");

    var done = new ManualResetEventSlim();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; done.Set(); };
    done.Wait();
    Console.WriteLine("kvd shutting down.");
    return 0;
}

int Set(string key, string value)
{
    if (!EnsureAlive()) return 2;
    using var client = new SluiceClient(Endpoint, exclusiveProducer: true);
    var payload = Encoding.UTF8.GetBytes(key + "\0" + value);
    var resp = client.Send(OpSet, payload);
    Console.WriteLine(resp.Text);
    return resp.Ok ? 0 : 1;
}

int Get(string key)
{
    if (!EnsureAlive()) return 2;
    using var client = new SluiceClient(Endpoint, exclusiveProducer: true);
    var resp = client.Send(OpGet, Encoding.UTF8.GetBytes(key));
    Console.WriteLine(resp.Text);
    return resp.Ok ? 0 : 1;
}

int Status()
{
    if (!EnsureAlive()) return 2;
    using var client = new SluiceClient(Endpoint, exclusiveProducer: true);
    var resp = client.Send(OpStatus, ReadOnlySpan<byte>.Empty);
    Console.WriteLine(resp.Text);
    return 0;
}

bool EnsureAlive()
{
    if (SluiceDiscovery.IsAlive(Endpoint, staleAfter)) return true;
    Console.Error.WriteLine("no live kvd daemon found — start one with:  kvd serve");
    return false;
}
