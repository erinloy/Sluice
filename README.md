# Sluice

[![CI](https://github.com/erinloy/Sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/erinloy/Sluice/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%2010.0-512BD4.svg)](https://dotnet.microsoft.com)
[![platforms](https://img.shields.io/badge/platforms-Windows%20%7C%20Linux-informational.svg)](docs/deployment.md)

**Zero-serialization, zero-copy shared-memory RPC for .NET.**

A *sluice* is a channel with a gate that controls fast flow. Sluice is a memory-mapped ring (the channel)
with a wait-handle doorbell (the gate): messages flow between processes on one machine and are **read in
place** — the bytes sitting in shared memory *are* the object. No serialize on send, no deserialize on
receive, no allocation on the hot path.

It does two things:

1. **Point-to-point RPC** — the **daemon + thin-CLI** pattern: a long-lived process holds expensive in-memory
   state, and short-lived command-line invocations discover it and exchange request/response messages — far
   cheaper than re-spinning that state per call, and far faster than HTTP, gRPC, or named pipes.
2. **A multi-way fabric** — broadcast (1→N), a many-to-many bus (N↔N), and an addressed, full-duplex peer
   mesh, all over a Disruptor-style multicast ring with selectable **reliable** or **lossy** delivery.

> Status: working, tested (39 tests green), benchmarked. **Cross-platform — verified on both**: the full suite
> passes on Windows *and* in a .NET 10 Linux container (`/dev/shm` file-backed maps + polling doorbell), and all
> three libraries build clean for `net8.0` (the ACA LTS runtime) on Linux. On Windows the shared region is a
> named (page-file-backed) map with a wait-handle doorbell; on Linux it is a `/dev/shm` (tmpfs) file-backed map
> with a polling doorbell — so it runs in Azure Container Apps (Linux) as well as on Windows. The framing,
> cursor, and discovery (named `Mutex` + lease file) logic is shared across both.

---

## Install

Sluice publishes to **GitHub Packages** (`Sluice`, `Sluice.Gossip`, `Sluice.Fusion`). Each push to `master`
ships a CI prerelease (`0.1.0-ci.<n>`); tagging `vX.Y.Z` ships the stable `X.Y.Z`. Both target **net8.0** and
**net10.0**.

GitHub Packages requires authentication even for reads, so add the feed once with a
[personal access token](https://github.com/settings/tokens) that has the `read:packages` scope:

```bash
dotnet nuget add source "https://nuget.pkg.github.com/erinloy/index.json" \
  --name sluice-github \
  --username <your-github-username> \
  --password <YOUR_PAT_WITH_read:packages> \
  --store-password-in-clear-text
```

Then reference it from any project:

```bash
dotnet add package Sluice          # core RPC + multi-way fabric
dotnet add package Sluice.Gossip   # epidemic dissemination (optional)
dotnet add package Sluice.Fusion   # attach/mirror/cache overlay (optional)
```

To pin the latest CI prerelease, add `--prerelease`. Or declare the source in a repo-local `nuget.config`
(commit this; tokens stay in your environment, not the file):

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="sluice-github" value="https://nuget.pkg.github.com/erinloy/index.json" />
  </packageSources>
  <packageSourceCredentials>
    <sluice-github>
      <add key="Username" value="%GITHUB_ACTOR%" />
      <add key="ClearTextPassword" value="%GITHUB_TOKEN%" />
    </sluice-github>
  </packageSourceCredentials>
</configuration>
```

Inside GitHub Actions the built-in `GITHUB_TOKEN` already authorizes the owner's feed — no PAT needed.

---

## Why it's different

Every other local-IPC option in .NET **copies and serializes**:

- **gRPC / MagicOnion** — HTTP/2 framing + protobuf/MessagePack encode/decode, even on loopback.
- **Named pipes** — a kernel stream you must frame and serialize over.
- **Cloudtoid.Interprocess** — a genuine shared-memory queue, but `Dequeue` **copies the message body out**
  of the ring into a pooled/allocated buffer (it clears the slot and advances the read cursor immediately, so
  it *cannot* hand back a stable view). You still serialize your object on top.

Sluice keeps the slot alive until you call `AdvanceRead()`, so the consumer gets a `ReadOnlySpan<byte>`
pointing **directly into the shared pages**. Combined with blittable message headers read via
`MemoryMarshal.AsRef`, the read path is a pointer reinterpret — there is nothing to deserialize.

---

## Benchmarks

Measured with BenchmarkDotNet v0.14.0 on Windows 11, .NET 10.0.9 (X64 RyuJIT AVX-512), Concurrent Server GC.
The serialization and multicast tables are the full default job (stable, in-process); allocation figures are
deterministic everywhere. See [docs/performance.md](docs/performance.md) for the full methodology and the
load-sensitivity caveat on cross-process latency. Reproduce with `dotnet run -c Release --project
bench/Sluice.Benchmarks -- --filter '*'`.

### Read in place vs. serialize (the core claim)

This is the cost Sluice's hot path **eliminates** — reading a message is a pointer reinterpret
(`MemoryMarshal.AsRef`), not a decode. Measured against decoding the same message with the common serializers:

| Reading one message       | Payload | Mean        | Allocated |
|---------------------------|--------:|------------:|----------:|
| **Blittable (in place)**  | **64 B** | **6.5 ns** | **0 B**   |
| MemoryPack deserialize    |    64 B |    107.2 ns |     272 B |
| MessagePack deserialize   |    64 B |    313.3 ns |     280 B |
| Json deserialize          |    64 B |    667.1 ns |     336 B |
| **Blittable (in place)**  | **1024 B** | **15.6 ns** | **0 B** |
| MemoryPack deserialize    |  1024 B |    392.1 ns |    2192 B |
| MessagePack deserialize   |  1024 B |    615.3 ns |    2200 B |
| Json deserialize          |  1024 B |  1,182.9 ns |    2576 B |

Reading a message in place is **~16× faster than MemoryPack**, **~100× faster than JSON**, and allocates
**nothing** (vs 270 B – 2.5 KB).

### End-to-end request → response (256-byte payload, across frameworks)

A full round trip through real shared memory / pipes (responder on a background thread; the payload still
crosses the OS boundary). The alternatives must serialize a message to carry the payload — Sluice sends it raw.

**Allocation — exact and load-independent:**

| Transport                       | Allocated | vs Sluice |
|---------------------------------|----------:|----------:|
| **Sluice (zero-alloc receive)** | **0 B**   | **0×**    |
| Sluice (convenience `Send`)     | 280 B     | 1.0×      |
| Cloudtoid + MemoryPack          | 1475 B    | 5.3×      |
| Cloudtoid + JSON                | 1772 B    | 6.3×      |
| Named pipe + MemoryPack         | 2072 B    | 7.4×      |
| Named pipe + JSON               | 2588 B    | 9.2×      |

**Latency (measured on a contended box — see the caveat below; ratios are a floor):** Sluice ~0.77 µs vs
Cloudtoid ~34 µs (~45×) vs named pipes ~116 µs (~150×). Sluice's shared-memory path stays sub-microsecond
under load while the syscall-based transports degrade, so its relative advantage *grows* on a busy machine.
The everywhere-true claims: **one to two orders of magnitude faster end-to-end**, and **5–9× less garbage — or
none** on the zero-alloc path.

> The 280 B is the response copied out across the API boundary by the convenience `Send` overload. The
> span-callback overload `Send<TState>(kind, payload, state, reader)` hands you the response **in place** and
> allocates **0 B/op** at essentially identical latency — the win is GC-pressure elimination. Use it on hot
> request paths to remove client-side allocation entirely.

### Multi-way fan-out (per-message cost)

The multicast ring's per-message cost — what sets the throughput ceiling. Read-in-place, zero allocation.

| Operation                          | 32 B     | 256 B    | Allocated |
|------------------------------------|---------:|---------:|----------:|
| Publish (fan-out write)            |  8.1 ns  | 13.8 ns  |     0 B   |
| Publish + consume in place         | 12.6 ns  | 17.9 ns  |     0 B   |

### Aggregate throughput (lossy broadcast, 64-byte messages)

`dotnet run -c Release --project bench/Sluice.Benchmarks -- throughput`

| Producers | Consumers | Messages/sec | GB/sec |
|----------:|----------:|-------------:|-------:|
| 1         | 0         |    77.6 M    |  5.9   |
| 1         | 1         |   135.6 M    | 10.3   |
| 1         | 4         |   114.3 M    |  8.7   |
| 2         | 2         |    26.4 M    |  2.0   |
| 4         | 4         |    16.9 M    |  1.3   |
| 4         | 1         |    18.1 M    |  1.4   |

Single-producer fan-out runs at **75–135M msgs/sec**; multiple producers contend on the one interlocked claim
counter (the natural cost of lock-free multi-producer ordering) and still sustain **16–26M msgs/sec aggregate**.
Unlike the syscall-based transports above, shared-memory throughput holds up under machine load.

### Lifecycle timings (init → bootstrapped → discovered → active → converged)

`dotnet run -c Release --project bench/Sluice.Benchmarks -- lifecycle`

| Subsystem | Stage | Median |
|-----------|-------|-------:|
| RPC       | init → bootstrapped (daemon)   | 0.9 ms  |
| RPC       | bootstrapped → discovered      | 6.6 ms  |
| RPC       | discovered → active (1st call) | 0.13 ms |
| Fusion    | host bootstrapped              | 4.0 ms  |
| Fusion    | mirror attach                  | 0.4 ms  |
| Fusion    | active (1st fetch)             | 0.1 ms  |
| Fusion    | invalidation propagation       | 9.5 ms  |
| Gossip    | init → all discovered (N=3..8) | ~31 ms  |
| Gossip    | seed → fully converged (N=3..8)| ~15 ms  |

Gossip timings are **flat from N=3 to N=8** — shared-memory multicast reaches every node at once (O(1)),
not the O(log N) rounds a network gossip protocol needs.

---

## Quick start

```csharp
// ---- Owner (the daemon that holds state) ----
using var server = new SluiceServer("my-endpoint", (in RpcContext ctx) =>
{
    // ctx.Request is a ReadOnlySpan<byte> pointing straight into shared memory — no copy, no decode.
    switch (ctx.Kind)
    {
        case 1: ctx.Reply(ctx.Request); break;              // echo
        default: ctx.Reply("unknown"u8, ok: false); break;
    }
});
server.Start();
SluiceDiscovery.Heartbeat("my-endpoint", 1 << 20);          // publish a lease so clients can find us

// ---- Client (a short-lived CLI invocation) ----
if (!SluiceDiscovery.IsAlive("my-endpoint", TimeSpan.FromSeconds(10)))
    return; // no daemon running

using var client = new SluiceClient("my-endpoint", exclusiveProducer: true);
RpcResponse resp = client.Send(kind: 1, "ping"u8);
Console.WriteLine(resp.Text);                                // "ping"
```

Streaming responses:

```csharp
foreach (var item in client.SendStream(kind: 3, request))
    Process(item);                                           // owner pushed N frames, ended with Complete()
```

### Multi-way: pub/sub, bus, and mesh

```csharp
// Broadcast (1→N) or many-to-many bus (N↔N) — any process opens the same topic by name.
using var topic = new ShmTopic("prices", maxPayload: 256, mode: DeliveryMode.Lossy);
var sub = topic.Subscribe();                                 // each subscriber sees every message
topic.Publish(quoteBytes);
if (sub.TryRead(out var span)) { Use(span); sub.Advance(); } // read in place

// Addressed, full-duplex peer mesh — every peer both sends and receives, no client/server roles.
using var alice = new ShmPeer("alice");
using var bob   = new ShmPeer("bob");
alice.Send("bob", "hello"u8);
bob.Receive(out var msg);                                    // -> "hello"
bob.Send("alice", "hi back"u8);                             // full duplex
```

`DeliveryMode.Reliable` never drops (the producer backpressures on the slowest subscriber);
`DeliveryMode.Lossy` never blocks (a lapped subscriber resyncs forward and reports `Dropped`).

### Extensions

**`Sluice.Gossip`** — epidemic dissemination: a replicated last-writer-wins key/value store that converges
across all processes. A local `Set` broadcasts the entry (O(1) on shared memory); periodic anti-entropy
re-gossips a random subset to repair lossy drops and bring late joiners up to date.

```csharp
using var node = new GossipNode("cluster", "node-a");
node.Start();
node.Set("x", "1"u8.ToArray());     // converges to every node; LWW by Lamport version
node.TryGet("x", out var value);
```

**`Sluice.Fusion`** — a Fusion-style attach/mirror/cache overlay (faithful to [ActualLab/Fusion](https://github.com/ActualLab/Fusion)).
A `ModelHost` owns values and publishes a tiny **invalidation** (key + version) on a change — never the value.
A `ModelMirror` caches what it reads and refetches lazily only when the origin invalidates it: the Fusion
"signal stale, recompute on access" lifecycle, peer-to-peer over shared memory.

```csharp
using var host = new ModelHost("prices");
host.Set("AAPL", quoteBytes);

using var mirror = new ModelMirror("prices");
SluiceComputed c = mirror.Get("AAPL");      // fetched once, then cached
// ... host.Set("AAPL", ...) → c.State becomes Invalidated → next Get refetches
using var state = new MirroredState(mirror, "AAPL");
state.Updated += v => Render(v);            // push feed, like Fusion's ComputedState<T>
```

---

## Try the demo

A tiny in-memory key-value store served by a daemon and driven by a thin CLI (`kvd`):

```
kvd serve              # start the daemon (holds the key-value state in memory)
kvd set foo bar        # a separate process: discovers the daemon, sets a key
kvd get foo            # -> bar     (read from the running daemon, not reloaded)
kvd status             # -> alive; keys=1; pid=12345
```

`set`/`get`/`status` never load the store — they reach into the already-running `serve` process over shared
memory. That is the efficiency win the whole library exists for.

---

## How it works

```
            ┌─────────────────────────── shared memory (memory-mapped file) ───────────────────────────┐
            │  header: [writeCursor | readCursor | capacity | magic]   data: [len|payload][len|payload]… │
            └────────────────────────────────────────────────────────────────────────────────────────────┘
 producer ──write payload, publish writeCursor (release)──▶              ◀──read in place, advance readCursor (release)── consumer
                                   doorbell: named EventWaitHandle (spin → block)
```

- **`ShmRing`** — an SPSC lock-free ring in a memory-mapped file. Monotonic 64-bit cursors; frames are
  4-byte aligned and never straddle the wrap (a skip-marker pads the tail). The consumer reads **in place**
  and only frees the slot on `AdvanceRead()`.
- **RPC** — one MPSC request ring (many clients → one owner; concurrent producers serialized by a named
  mutex, or lock-free in `exclusiveProducer` mode) plus a per-client SPSC response ring, correlated by a
  16-byte id in the blittable `RpcHeader`. Unary and streaming.
- **`ShmMulticast`** — the multi-way core: a Disruptor-style *sequenced* ring. Producers claim a monotonic
  sequence with one `Interlocked.Increment`, write their slot, then stamp the slot's sequence as the publish
  barrier. Each subscriber holds an independent cursor and reads every message in place. The per-slot stamp
  is also the lap detector that makes lossy delivery (and torn-read re-validation) possible. `ShmTopic` and
  `ShmPeer` are thin façades over it for pub/sub, bus, and addressed full-duplex mesh.
- **Discovery** — a system-global named mutex elects the single owner; a lease file (pid + heartbeat +
  capacity) lets a fresh CLI find the live instance and tell a crash from a running process by staleness.
  Peers announce presence the same way (`ShmPeer.IsPeerAlive`).

---

## Documentation

Deeper guides live in [`docs/`](docs/):

- **[Architecture](docs/architecture.md)** — the layers, the zero-copy ring protocol, the sequenced multicast
  ring, the OS floor, and the correctness model.
- **[Choosing a topology](docs/topologies.md)** — a decision table and a copy-paste cookbook for RPC, frame
  channels, topics, peers, gossip, and Fusion.
- **[Delivery & safety](docs/delivery-and-safety.md)** — reliable vs lossy, torn-read handling, the memory
  model, and crash robustness, failure mode by failure mode.
- **[Deployment & cross-platform](docs/deployment.md)** — Windows vs Linux internals, container `/dev/shm`
  sizing, and Azure Container Apps.
- **[Performance](docs/performance.md)** — the numbers, where they come from, and the knobs that move them.

Contributions: see [CONTRIBUTING.md](CONTRIBUTING.md). Security: see [SECURITY.md](SECURITY.md). Release
history: [CHANGELOG.md](CHANGELOG.md).

---

## Design notes & honest limitations

- **Cross-platform, with one honest asymmetry: the doorbell.** Windows backs the region with a named
  (page-file) map and signals readers via a named `EventWaitHandle` (sub-µs wakeups). Linux — where named maps
  and named wait handles aren't supported in .NET — backs it with a `/dev/shm` (tmpfs) file map and has **no OS
  doorbell**, so a blocked reader polls with an escalating backoff capped at ~1 ms. Throughput and the in-place
  read path are identical; only the *idle→first-message* wakeup latency differs (spin-hot for the first ~64
  iterations, then ≤1 ms). Named `Mutex` (discovery + the MPSC producer lock) works on both. The owner unlinks
  its tmpfs file on dispose. This is what lets Sluice run in Azure Container Apps (Linux).
- **`exclusiveProducer`** skips the cross-process producer mutex (a kernel transition per send) for the
  common single-CLI case. It still syncs the shared cursor — it only drops the *mutex*, never correctness —
  but you must not have two "exclusive" producers writing concurrently.
- **Crash robustness.** A client that dies mid-request can't crash the daemon (each request is isolated).
  The named mutex surfaces `AbandonedMutexException` as inherited ownership, so a crashed owner's endpoint is
  reclaimable.
- **Response-ring cache** is bounded (FIFO, 256) so a stream of short-lived clients can't leak handles; an
  evicted persistent client just pays one re-open.
- **Reliable multicast trusts its subscribers.** A registered subscriber that stalls (or crashes without
  unsubscribing) backpressures every producer on that topic. Use `Reliable` only with cooperative consumers;
  `Lossy` is crash-robust by design (a dead subscriber is simply lapped). A subscriber lease/timeout to evict
  the dead automatically is on the roadmap.
- **Lossy in-place reads can tear** if a producer laps you mid-read; the `TryReadCopy` / `ShmPeer` /
  `ShmTopic` convenience paths re-validate the sequence stamp after copying, so use those for lossy unless you
  validate yourself.
- **Container `/dev/shm` sizing (Linux).** A ring's backing file lives in tmpfs, which counts against the
  container's shared-memory limit (often a 64 MB default). Size the total of your rings under that limit, or
  raise it — in Azure Container Apps via an `EmptyDir` volume with `storageType: Memory` mounted at `/dev/shm`,
  or in Docker via `--shm-size`. If `/dev/shm` is absent, Sluice falls back to the temp dir (real disk).
- **Not** a network transport, and **not** a durable queue — it's the fastest path between processes on one
  box. Pair it with a durable store (FASTER, a log, etc.) if you need persistence.

## Layout

```
src/Sluice                 the library
  ShmRing.cs               SPSC zero-copy ring (RPC transport)
  ShmMulticast.cs          Disruptor-style multi-way ring (broadcast / bus / mesh core)
  ShmMap.cs                the one OS split: named map (Windows) vs /dev/shm file map (Linux)
  Rpc/                     SluiceServer, SluiceClient, discovery
  Multiway/                ShmTopic (pub/sub + bus), ShmPeer (addressed full-duplex mesh)
  IFrameChannel.cs         frame transport seam (drop-in for a duplex Stream, zero-copy on read)
  ShmFrameChannel.cs       bidirectional duplex channel = two rings
  ShmFrameListener.cs      Accept() — multiplex many client connections onto one daemon
src/Sluice.Gossip          epidemic dissemination (GossipNode: LWW store, rumor + anti-entropy)
src/Sluice.Fusion          Fusion-style overlay (ModelHost/ModelMirror/MirroredState, lazy invalidation)
tests/Sluice.Tests         24 xUnit tests (ring, RPC unary/stream/MPSC, multicast MP-claim/lossy/reliable,
                           broadcast/bus/mesh/duplex, discovery)
bench/Sluice.Benchmarks    BenchmarkDotNet vs Cloudtoid + named pipes, serialization microbench, multicast
                           per-op, and the `throughput` aggregate harness
samples/Sluice.Demo        the `kvd` daemon + thin-CLI demo
```

## License

MIT — see [LICENSE](LICENSE).
