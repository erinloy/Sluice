# Choosing a topology

Sluice gives you six communication shapes over the same shared-memory core. This guide is the decision tree and
a copy-paste cookbook for each. All examples are real, current API.

## Decision table

| You want…                                                        | Use            | Pattern        | Delivery        |
|------------------------------------------------------------------|----------------|----------------|-----------------|
| A long-lived daemon that short-lived CLIs call (request/response) | `SluiceServer` / `SluiceClient` | point-to-point RPC | reliable        |
| The same, but streaming a sequence back per call                 | RPC `SendStream` | server-streaming | reliable        |
| A raw duplex byte-frame pipe (e.g. LSP, custom protocol)         | `ShmFrameChannel` + `ShmFrameListener` | duplex frames | reliable        |
| One producer fanning out to many readers                         | `ShmTopic`     | broadcast 1→N  | your choice     |
| Many producers, many readers, all on one channel                 | `ShmTopic`     | bus N↔N        | your choice     |
| Named peers messaging each other directly                        | `ShmPeer`      | addressed mesh | reliable (default) |
| A replicated key/value store that converges across processes     | `GossipNode`   | epidemic       | lossy + repair  |
| Processes that mirror & cache each other's data models           | `ModelHost` / `ModelMirror` | invalidate+fetch | reliable inval  |

Two quick rules of thumb:

- **Need a *reply*?** → RPC. **Need a *stream of bytes both ways*?** → frame channel. **Need *fan-out*?** → topic.
- **Reliable** when you must not lose a message and your consumers are cooperative; **Lossy** when latency and
  crash-robustness matter more than completeness (market data, telemetry, presence). See
  [delivery-and-safety](delivery-and-safety.md).

---

## RPC — the daemon + thin-CLI pattern

The flagship use case: a process holds expensive in-memory state; short-lived commands discover it and exchange
messages instead of re-spinning that state per call.

**Daemon:**

```csharp
using Sluice.Rpc;

const int OpGet = 1, OpSet = 2;

using var owner = SluiceDiscovery.TryBecomeOwner("mydaemon");
if (owner is null) return; // another instance already owns the endpoint

var store = new ConcurrentDictionary<string, string>();
using var server = new SluiceServer("mydaemon", (in RpcContext ctx) =>
{
    switch (ctx.Kind)
    {
        case OpSet:
            var text = Encoding.UTF8.GetString(ctx.Request);   // request span is mapped in place
            var i = text.IndexOf('\0');
            store[text[..i]] = text[(i + 1)..];
            ctx.Reply("ok"u8);
            break;
        case OpGet:
            var key = Encoding.UTF8.GetString(ctx.Request);
            ctx.Reply(store.TryGetValue(key, out var v)
                ? Encoding.UTF8.GetBytes(v) : "(none)"u8, ok: store.ContainsKey(key));
            break;
    }
});
server.Start();
SluiceDiscovery.Heartbeat("mydaemon", 1 << 20);                 // refresh periodically too
```

**Thin CLI (a fresh process):**

```csharp
if (!SluiceDiscovery.IsAlive("mydaemon", TimeSpan.FromSeconds(10))) { /* start the daemon */ }

using var client = new SluiceClient("mydaemon", exclusiveProducer: true);
var resp = client.Send(OpSet, Encoding.UTF8.GetBytes("hello\0world"));
Console.WriteLine(resp.Text);                                   // "ok"
```

`exclusiveProducer: true` takes the lock-free request path — correct when you're the only process publishing
right now (the common single-CLI case). Leave it `false` when several clients may publish concurrently.

A complete, runnable version of this is the [`kvd` sample](../samples/Sluice.Demo).

### Streaming a response

```csharp
// server: emit many, then finish
case OpRange:
    int n = BitConverter.ToInt32(ctx.Request);
    for (int i = 0; i < n; i++) ctx.StreamItem(BitConverter.GetBytes(i));
    ctx.Complete();
    break;

// client: enumerate until the stream ends
foreach (var item in client.SendStream(OpRange, BitConverter.GetBytes(50)))
    Console.WriteLine(BitConverter.ToInt32(item));
```

### Zero-allocation receive

When the response is hot and you want no managed garbage, read it in place:

```csharp
client.Send(OpGet, keyBytes, state: buffer, static (ok, span, buf) =>
{
    // span points into shared memory; copy out only what you keep
    span.CopyTo(buf);
});
```

The `static` lambda + `TState` mean no per-call closure allocation; the response is never copied into an
`RpcResponse`. Measured at **0 B/op**. See [performance](performance.md).

---

## Frame channel — duplex byte-frame pipe

For a protocol that is already self-delimited and wants a raw bidirectional pipe (the canonical case is an LSP
multiplexer). It is the drop-in for a duplex `NamedPipeStream`, but frame-native and zero-copy on read.

```csharp
// daemon: accept many client connections, each its own duplex channel
using var listener = new ShmFrameListener("lsp");
while (!ct.IsCancellationRequested)
{
    IFrameChannel conn = listener.Accept(ct);                   // blocks until a client connects
    _ = Task.Run(() => Serve(conn));                            // one channel per connection
}

// client
using IFrameChannel ch = ShmFrameChannel.Connect("lsp");
ch.WriteFrame(requestBytes);
if (ch.WaitForFrame()) {                                        // block until a frame arrives
    while (ch.TryReadFrame(out var frame)) { Handle(frame); ch.AdvanceFrame(); }
}
```

`TryReadFrame` hands back a span into the ring, valid until `AdvanceFrame()` — the same read-in-place contract
as the underlying ring, preserved all the way to your protocol parser.

---

## Topic — broadcast and bus

Any process opens the same topic by name; the first creates it. One publisher → broadcast; many → a bus. Every
subscriber sees every message, each at its own pace.

```csharp
using Sluice.Multiway;

using var topic = new ShmTopic("prices", maxPayload: 256, mode: DeliveryMode.Lossy);

// publisher(s)
topic.Publish(quoteBytes);

// subscriber(s) — independent cursor
var sub = topic.Subscribe();
if (sub.Wait()) {                                              // block until something is available
    while (sub.TryReadCopy(out var msg)) Handle(msg);          // safe copy (correct under lossy)
}
Console.WriteLine($"dropped so far: {sub.Dropped}");
```

Use `sub.TryRead(out span)` for the zero-copy in-place read when delivery is `Reliable` (or when you handle
tearing yourself); use `TryReadCopy` under `Lossy`. Pick `maxPayload` for your largest message and `slotCount`
(ring depth) for how far a slow subscriber may lag before being lapped.

---

## Peer — addressed full-duplex mesh

Named peers; each owns an inbox and can both send and receive — no client/server roles.

```csharp
using var alice = new ShmPeer("alice");
using var bob   = new ShmPeer("bob");

alice.Send("bob", "hello"u8);
bob.Receive(out var msg);            // blocks → "hello"
bob.Send("alice", "hi back"u8);      // full duplex

if (ShmPeer.IsPeerAlive("bob", TimeSpan.FromSeconds(5))) { /* … */ }
```

`Send` caches the outbox to each peer; the inbox ring is multi-producer (interlocked claim), so many peers can
message the same inbox safely. Defaults to `Reliable`.

---

## Gossip — converging key/value store

A replicated LWW store that spreads updates epidemically and repairs drops via anti-entropy.

```csharp
using Sluice.Gossip;

using var node = new GossipNode("cluster", "node-a");
node.KeyUpdated += key => Console.WriteLine($"{key} changed");
node.Start();

node.Set("leader", "node-a"u8);                                // gossips immediately
if (node.TryGet("leader", out var who)) { /* … */ }
Console.WriteLine($"peers: {node.LivePeerCount}, discovered: {node.IsDiscovered}");
```

Because the fan-out is shared-memory multicast, convergence latency is flat as the cluster grows (it's the
[lifecycle benchmark](performance.md): N=3..8 is constant). Conflicts resolve by `(Lamport version, origin)`.

---

## [Fusion](https://github.com/ActualLab/Fusion) — mirror & cache a data model

A process publishes a model; others mirror keys lazily and stay fresh via invalidations. The origin sends
*staleness*, never values — readers fetch on demand.

```csharp
using Sluice.Fusion;

// origin
using var host = new ModelHost("market");
host.Set("BTC", priceBytes);                                   // invalidates every mirror's "BTC"

// consumer
using var mirror = new ModelMirror("market");
SluiceComputed c = mirror.Get("BTC");                          // fetches once, caches
// … host.Set("BTC", …) later → c.State becomes Invalidated; next Get refetches

// typed, with a codec (MemoryPack shown)
var codec = new SluiceCodec<Quote>(
    q => MemoryPackSerializer.Serialize(q),
    span => MemoryPackSerializer.Deserialize<Quote>(span)!);
using var typedHost = new ModelHost<Quote>("market", codec);
using var typedMirror = new ModelMirror<Quote>("market", codec);
typedHost.Set("BTC", new Quote(100, 101));
Quote q = typedMirror.Get("BTC").Value;

// a push feed of one key
using var state = new MirroredState<Quote>(typedMirror, "BTC");
state.Updated += quote => Render(quote);
```

See the [architecture doc](architecture.md#layer-3--extensions) for how Fusion composes a topic (invalidate)
with RPC (fetch).

---

## Sizing cheat-sheet

| Knob              | Where                         | Guidance                                                            |
|-------------------|-------------------------------|---------------------------------------------------------------------|
| `capacity`        | `ShmRing` / RPC / frame       | bytes of the data region; default 1 MiB. Bigger = more in-flight headroom. |
| `maxPayload`      | `ShmTopic` / `ShmPeer`        | your largest single message; slots are fixed at this size.          |
| `slotCount`       | `ShmTopic` / multicast        | ring depth (power of two). How far a slow lossy subscriber may lag before being lapped. |
| `maxConsumers`    | `ShmTopic` / multicast        | max concurrent subscribers (default 64).                            |
| `exclusiveProducer` | `SluiceClient`              | `true` for the single-writer lock-free path; `false` when many clients publish concurrently. |

Total shared memory ≈ `HeaderSize + capacity` per ring, and `headers + slotCount × slotSize` per multicast ring.
On Linux this counts against the container's `/dev/shm` budget — see [deployment](deployment.md).
