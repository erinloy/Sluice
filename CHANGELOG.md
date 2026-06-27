# Changelog

All notable changes to Sluice are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/erinloy/Sluice/commits/master
