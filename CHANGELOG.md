# Changelog

All notable changes to Sluice are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`Sluice.Supergraph`** — a **federated reactive graph**: `Sluice.Fusion`'s invalidate→refetch model generalized
  over the fabric so it spans hosts. A `GraphPeer` owns `SourceVertex`/`ComputedVertex` vertices and `Observe`s
  others by `VertexId` (`owner participant + key`). A read of a vertex you own is a direct local read; a remote one
  routes a correlated fetch over the fabric (shared-memory or network); a change broadcasts a tiny invalidation
  across the whole federation. A `ComputedVertex` whose dependencies live on other participants recomputes and
  re-publishes when they change, so change propagates transitively — many processes form one logical reactive
  graph. Self-healing under message loss: lost fetches retry, and an invalidation high-water mark prevents a
  fetch from caching a value older than an invalidation that raced ahead of it. (`samples/Sluice.SupergraphDemo`,
  [docs](docs/supergraph.md).)
- **`IChannel.Self`** — a channel now exposes the `ParticipantId` it publishes as (the address peers reply to). The
  primitive a reply-routing layer (RPC, the supergraph's fetch) needs to address messages back to itself.

### Fixed

- **`ShmPeer.Send` is now thread-safe.** The outbox cache was a plain `Dictionary` mutated without
  synchronization; concurrent sends from multiple threads (e.g. a fabric pump replying while a worker thread sends
  a request) could corrupt it. Guarded by a lock around the cache lookup (the ring's own publish stays lock-free).
- **A throwing `ChannelHandler` no longer deafens a channel.** The shared-memory and TCP transport pumps now
  isolate each delivery, so one bad handler call drops only its own message instead of killing the pump thread.

## [0.1.0] - 2026-06-26

Initial public release. Sluice is a zero-serialization, zero-copy, read-in-place shared-memory transport for
.NET — same-host IPC where the bytes sitting in shared memory *are* the object.

### Core

- **`ShmRing`** — single-producer/single-consumer lock-free ring with read-in-place semantics: the consumer
  receives a `ReadOnlySpan<byte>` into the mapped pages, valid until `AdvanceRead()`. Monotonic 64-bit cursors,
  4-byte-aligned framing with a wrap-skip marker, cache-line-separated cursors, optional wait-handle doorbell.
- **`ShmMulticast`** — many-to-many Disruptor-style sequenced ring: interlocked-claim publish, per-slot sequence
  stamp as the publish barrier and lap detector, per-subscriber cursors. Selectable **Reliable** (backpressure,
  never drop) or **Lossy** (overwrite-oldest, resync, `Dropped` count) delivery; `TryReadCopy` re-validates the
  stamp for torn-read safety under lossy.
- **`ShmMap`** — the single OS abstraction: named page-file-backed map + named wait-handle doorbell on Windows;
  `/dev/shm` (tmpfs) file-backed map + polling on Linux, with `FileMode.CreateNew` arbitrating the creation race.

### RPC & transport

- **`SluiceServer` / `SluiceClient`** — the daemon + thin-CLI pattern: MPSC request ring, per-client response
  rings correlated by id, unary + server-streaming. Handler runs synchronously over the in-place request span
  (`RpcContext` is a `ref struct`). `exclusiveProducer` lock-free path for the single-writer case.
- **Zero-allocation receive** — `Send<TState>(kind, payload, state, reader)` delivers the response as an in-place
  span (no `RpcResponse`, no copy, no closure). Measured 0 B/op.
- **`SluiceDiscovery`** — named-`Mutex` owner election + lease-file heartbeat; distinguishes a crashed owner from
  a live one and reclaims via `AbandonedMutexException`.
- **`IFrameChannel` / `ShmFrameChannel` / `ShmFrameListener`** — a duplex, frame-native, zero-copy-on-read
  transport (drop-in for a duplex named pipe) with an `Accept` multiplexer for daemons serving many connections.

### Multi-way fabric

- **`ShmTopic`** — broadcast (1→N) and bus (N↔N) pub/sub by name.
- **`ShmPeer`** — addressed, full-duplex peer mesh with presence discovery.
- **Reliable-subscriber lease eviction** — each subscriber carries a heartbeat lease (`LeaseMs`, default 30 s);
  a reliable producer reclaims a crashed subscriber's cell instead of backpressuring on it forever.

### Federation fabric (`Sluice.Fabric`)

- A transport-agnostic seam — `ParticipantId`, `IChannel` (broadcast + addressed send + membership), `ISignal`
  (the doorbell generalized), `ITransport` — so one channel can hold local and remote participants alike.
- **`ShmTransport`** — the local realization (zero-copy, over `ShmTopic` + `ShmPeer`).
- **`TcpTransport`** — the remote realization: channels over TCP, length-prefixed frames multiplexed per
  channel, hello-based peer identification. (No TLS / auto-reconnect yet.)
- **`FederatedTransport`** — composes a local + remote transports into one channel, so a single `Broadcast`
  reaches co-located shared-memory participants *and* remote participants from the same call.

### Extensions

- **`Sluice.Gossip`** — `GossipNode`: a replicated last-writer-wins key/value store with rumor broadcast +
  anti-entropy repair + presence membership; convergence latency flat across cluster size.
- **`Sluice.Fusion`** — a Fusion-style attach/mirror/cache overlay (`ModelHost` / `ModelMirror` /
  `MirroredState`) that publishes invalidations (not values) over a topic and fetches over RPC; typed generics
  (`ModelHost<T>` etc.) via a `SluiceCodec<T>` serializer pair with span-based deserialize.

### Platform & packaging

- Cross-platform: verified green on Windows and in a .NET 10 Linux container; libraries multi-target
  `net8.0;net10.0` (net8 is the Azure Container Apps LTS runtime).
- Published to GitHub Packages via CI (build + test on Windows and Linux, then pack + push on green).
- **SourceLink + deterministic build** — the package nuspec carries the repository URL and commit; debuggers
  step straight to source.

### Quality & tooling

- **Microsoft Coyote systematic-concurrency tests** for the ring protocol (`tests/Sluice.Concurrency`): the
  scheduler explores the producer/consumer interleavings exhaustively and proves no schedule loses, reorders,
  or tears a message. Verified to catch an injected race, so a green run is a real proof. Runs in CI as the
  `concurrency-check` job.
- BenchmarkDotNet suite vs Cloudtoid + named pipes, serialization microbench, multicast per-op, throughput and
  lifecycle harnesses.

[0.1.0]: https://github.com/erinloy/Sluice/releases/tag/v0.1.0
