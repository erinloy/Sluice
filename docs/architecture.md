# Architecture

This is the "how it actually works" document — the mechanisms underneath Sluice's headline claim of
**zero-serialization, zero-copy, read-in-place** cross-process messaging. If you just want to use the library,
the [README](../README.md) and the [topology guide](topologies.md) are enough; read this when you want to
trust it, extend it, or port it.

## The one idea

Every other local-IPC option in .NET moves *bytes* between processes and then asks you to *encode and decode*
an object on top of those bytes. Sluice moves nothing. The message lives in a page that is mapped into both
processes' address spaces, and the reader is handed a `ReadOnlySpan<byte>` that points **directly at that
page**. A blittable header is reinterpreted in place with `MemoryMarshal.AsRef<T>`; the payload is whatever you
put there. There is no "receive" copy and no deserialize step on the hot path.

The whole library is the disciplined machinery required to make that safe across processes that don't share a
heap, a GC, or a thread scheduler.

## The layers

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Extensions                                                                │
│    Sluice.Gossip   GossipNode (LWW store)  ─────────────┐                  │
│    Sluice.Fusion   ModelHost / ModelMirror / MirroredState (+ typed<T>)    │
│                      invalidate over a topic, fetch over RPC ──────┐       │
├───────────────────────────────────────────────────────────┐       │       │
│  Multi-way fabric                                          │       │       │
│    ShmTopic   pub/sub (1→N) + bus (N↔N)  ◄─────────────────┘◄──────┘       │
│    ShmPeer    addressed full-duplex mesh                                   │
├──────────────────────────────┬────────────────────────────────────────────┤
│  Point-to-point RPC          │  Frame transport                            │
│    SluiceServer (daemon)     │    IFrameChannel  (duplex, frame-native)    │
│    SluiceClient (caller)     │    ShmFrameChannel (two rings)              │
│    SluiceDiscovery (election)│    ShmFrameListener (Accept multiplexer)    │
│    RpcHeader (blittable)     │                                             │
├──────────────────────────────┴────────────────────────────────────────────┤
│  Shared-memory rings                                                       │
│    ShmRing       SPSC, zero-copy, optional doorbell                        │
│    ShmMulticast  MPMC, Disruptor-style sequenced slots, reliable/lossy     │
├────────────────────────────────────────────────────────────────────────────┤
│  OS floor                                                                  │
│    ShmMap   named map + wait-handle doorbell (Windows)                     │
│             /dev/shm file map + polling      (Linux)                       │
└────────────────────────────────────────────────────────────────────────────┘
```

The graph is acyclic: each layer depends only on the ones below it. There are exactly **two** ring primitives
because there are two fundamentally different access patterns — point-to-point (one writer, one reader) and
multicast (many writers, many independent readers) — and they share the `ShmMap` floor rather than each
re-implementing the OS split.

---

## Layer 0 — `ShmMap`: the OS floor

Named shared memory and named wait handles are Windows-only in .NET. `ShmMap` is the single place that knows
this, so nothing above it carries a platform `#if`.

| Concern            | Windows                                  | Linux / Unix                                            |
|--------------------|------------------------------------------|--------------------------------------------------------|
| Shared region      | named, page-file-backed MMF              | file-backed MMF under `/dev/shm` (tmpfs → still RAM)    |
| Create-or-fail     | `MemoryMappedFile.CreateNew` (throws if exists) | `FileMode.CreateNew` on the backing file (same atomicity) |
| Doorbell           | named `EventWaitHandle`                   | none → callers poll                                    |
| Mutual exclusion   | named `Mutex` (also works on Unix)       | named `Mutex`                                          |
| Cleanup            | map vanishes with last handle            | the owner unlinks the tmpfs file on dispose            |

The framing, cursor arithmetic, memory barriers, and slot logic above this layer are byte-identical across
platforms. Only the *idle wakeup* differs: Windows blocks on the wait handle; Linux polls with an escalating
backoff capped at ~1 ms. Throughput and the in-place read path are the same on both.

> **Why a file under `/dev/shm` and not a real file?** `/dev/shm` is tmpfs — a RAM-backed filesystem — so a
> "file-backed" map there never touches a disk. It is the standard Linux mechanism for named shared memory
> when POSIX named maps aren't available to you. If `/dev/shm` is absent, Sluice falls back to the temp dir
> (which *is* disk-backed); see [deployment](deployment.md) for sizing this in a container.

---

## Layer 1a — `ShmRing`: the SPSC zero-copy ring

A single-producer / single-consumer lock-free ring living in one mapped region. This is the transport under
RPC and the frame channel.

### Memory layout

```
 0      64     128   136                256                       256 + capacity
 ├──────┼──────┼─────┼─────┬─────────────┼──────────── data region ───────────┤
 │write │read  │ cap │magic│   (pad)     │  [int len][payload…] [int len]…    │
 │cursor│cursor│     │     │             │   each frame 4-byte aligned        │
 └──────┴──────┴─────┴─────┴─────────────┘
   cache-line 0   cache-line 1
```

The two cursors sit on separate 64-byte cache lines so the producer and consumer never false-share. Cursors
are **monotonic 64-bit byte counts** — they never wrap; the physical slot is `cursor % capacity`. This makes
"is there space / is there data" a subtraction (`write - read`) that can never be ambiguous about a full-vs-empty
ring, which the classic wrapped-index ring can't do without wasting a slot.

### Framing and the wrap-skip

Each frame is `[int length][payload…]`, padded to a 4-byte boundary. A frame is never split across the physical
wrap boundary: when the bytes remaining to the end of the buffer can't hold the frame, the producer writes a
`SkipMarker` (`length == -1`) filler and jumps to offset 0. The consumer sees the marker and does the same
jump. This is what lets the reader hand back **one contiguous span** for every message — the payload is always
physically contiguous, so a `ReadOnlySpan<byte>` over it is always valid.

### The read-in-place protocol

```
producer                                 consumer
────────                                 ────────
TryWrite(payload):                       TryRead(out span):
  check free space vs cached read cursor   check data vs cached write cursor
  write [len][payload] at write%cap        span = view into the mapped page  ◄── no copy
  Volatile.Write(writeCursor)  ──┐         (use span…)
       release barrier           └──acquire─► AdvanceRead():
                                              Volatile.Write(readCursor)
                                                 release barrier (frees the slot)
```

The contract is `if (TryRead(out span)) { /* use span */ AdvanceRead(); }`. The slot stays alive — the
producer cannot reuse it — **until** `AdvanceRead()`. That single decision is the whole difference from
[`Cloudtoid.Interprocess`](https://github.com/cloudtoid/interprocess), whose `Dequeue` clears the slot and advances the read cursor immediately, so it
*must* copy the body out before returning. Sluice defers the advance to the caller, so it can hand back a
stable view.

**Cursor caching.** Each side keeps a private cache of the *other* side's cursor and only re-reads the shared
cursor (an acquire-barrier load) when its local view says the ring looks full/empty. On the steady-state hot
path neither side touches the other's cache line, which is most of why a round trip stays sub-microsecond.

**The doorbell.** A reader can spin (poll `TryRead`) or block. `WaitToRead` spins briefly, then on Windows
blocks on a named auto-reset `EventWaitHandle` that the producer `Set`s after each write; on Linux (no
doorbell) it polls with a capped backoff. The ring is *correct* without the doorbell — it's purely a
latency/CPU trade for the blocking style.

### One ring, many sequential producers (the MPSC twist)

The RPC request ring has one owner-reader but *many* client processes writing to it over time. A fresh CLI
process starts with a zeroed local write cursor while the shared cursor has already advanced. `SyncProducerCursor`
reloads the shared cursor before a write. Concurrent writers are additionally serialized by a named `Mutex`
(skippable via `exclusiveProducer` when you know there's only one writer right now — see
[performance](performance.md)). The mutex guards *concurrency*; the cursor sync guards *sequential* processes —
they are independent, and the exclusive path drops only the mutex, never the sync.

---

## Layer 1b — `ShmMulticast`: the sequenced MPMC ring

Broadcast and bus need a different shape: many producers, and many *independent* readers that each see **every**
message. This is a [Disruptor](https://lmax-exchange.github.io/disruptor/)-style sequenced ring.

```
 header: [claim cursor][mode][slotCount][slotSize][maxConsumers][magic][consumer cells…]
 slots:  each slot = [long sequence][int length][payload…], fixed size, count = power of two
```

### Publish: claim, write, stamp

```
seq   = Interlocked.Increment(claimCursor)   // globally unique, monotonic — this is the lock-free MP claim
slot  = seq & mask                           // power-of-two ring index
wait until slot's previous occupant is consumed   (reliable mode only — see below)
write [length][payload] into the slot
Volatile.Write(slot.sequence = seq)          // RELEASE barrier == the publish point
```

The per-slot `sequence` stamp does double duty: it is the **publish barrier** (a reader that sees
`slot.sequence == itsExpectedSeq` knows the payload bytes are fully written) and the **lap detector** (a stamp
greater than expected means the slot was reused by a newer message — the reader was lapped).

### Subscribe: per-reader cursor, read in place

Each subscriber owns a cursor and a "consumer cell" in the header (so producers can see how far the slowest
reader has progressed, for reliable gating). `TryRead`:

```
stamp = Volatile.Read(slot.sequence)          // acquire
if stamp == next  → payload = view into slot; ready          (in place, no copy)
if stamp <  next  → not published yet → false
if stamp >  next  → we were lapped → resync forward to the oldest live message, count the gap in Dropped
```

### Reliable vs lossy — one ring, a delivery switch

- **Reliable**: before reusing a slot, the producer waits until every registered subscriber has passed it. No
  message is ever dropped — but a stalled subscriber backpressures *every* producer on the ring. Use only with
  cooperative consumers.
- **Lossy**: the producer never waits on consumers; it overwrites the oldest slot. A lapped subscriber detects
  the gap (its slot's stamp jumped) and resyncs forward, reporting `Dropped`. Lowest latency, crash-robust — a
  dead subscriber is simply lapped, never a liability.

Because a lossy producer can overwrite a slot *while* a reader is reading it, the in-place `TryRead` can tear
under lossy delivery. `TryReadCopy` exists for exactly this: it copies the payload out, then **re-validates the
slot's sequence stamp**; if the slot was clobbered mid-copy it counts a drop and retries. The convenience paths
(`ShmTopic` consumers that need safety, `ShmPeer`, gossip) use `TryReadCopy`. Use in-place `TryRead` for lossy
only when you can tolerate or detect the tear yourself; it's always safe under reliable delivery.

### `CreateOrAttach` and the creation race

Any peer in a bus can start first. `CreateOrAttach` uses `ShmMap.CreateNew` (atomic create-or-fail) to elect a
single creator that initializes the header geometry; everyone else catches the failure and `Open`s, inheriting
the creator's parameters. A brief retry covers the window where the creator has created the region but not yet
stamped the magic. (On Linux the same race is arbitrated by `FileMode.CreateNew` on the backing file.)

---

## Layer 2 — RPC, frame transport, discovery

### RPC

The `RpcHeader` is a 32-byte blittable struct (`[Guid CorrelationId][long ClientId][int Kind][RpcFlags]`),
reinterpreted in place. The wire frame is `[RpcHeader][payload]`.

```
client                         shared rings                      owner (SluiceServer)
──────                         ────────────                      ────────────────────
SluiceClient(endpoint):
  create OWN response ring  ───────────────────────────────────► (opened on first reply)
  open shared request ring
Send(kind, payload):
  write [header|payload] ──►  request ring (MPSC) ──► drain ──► SluiceHandler(in RpcContext)
  await response ring                                            ctx.Reply / StreamItem / Complete
  ◄───────────────────────  this client's response ring ◄──────  write [header|payload]
  correlate by CorrelationId,
  drop stray frames
```

- The handler runs **synchronously over the in-place request span** — that's the zero-deserialize hot path. The
  `RpcContext` is a `ref struct` so the span cannot escape onto the heap or across an `await`.
- Each client has its *own* response ring, so replies route straight to the caller with no demultiplexing. The
  server caches opened response rings with a bounded FIFO (a flood of short-lived CLI clients can't leak
  handles; an evicted client pays one re-open).
- `Send<TState>` is the zero-allocation receive: instead of copying the response into an `RpcResponse`, it hands
  your callback the in-place span (`TState` threads context in without a closure). See [performance](performance.md).
- Streaming is `StreamItem*` then `Complete`, surfaced as `IEnumerable<byte[]>` on the client, correlated by the
  same id with a terminal `StreamEnd` flag.
- A bad request can't take down the daemon — each dispatch is isolated and surfaced via `OnError`.

### Frame transport

A parallel, *non*-correlated transport for protocols that are already self-framed (LSP's `Content-Length`
messages). `IFrameChannel` is a duplex, frame-native, zero-copy-on-read seam; `ShmFrameChannel` implements it as
**two `ShmRing`s, one per direction**. `ShmFrameListener.Accept` is the multiplexer — a connecting client mints a
unique id, creates its per-connection rings, and posts the id on an accept ring; `Accept` returns the server side
of that fresh duplex channel. This is the Sluice analogue of `NamedPipeServerStream.WaitForConnection`, and it is
a *separate* model from RPC on purpose: RPC gives you request/response correlation; the frame channel gives you a
raw duplex byte-frame pipe.

### Discovery

`SluiceDiscovery` is the daemon-finds-and-elects layer, mirroring a proven named-mutex + lease-file pattern:

- **Election**: a named `Mutex` (`TryBecomeOwner`). An `AbandonedMutexException` means the previous owner
  crashed → the new caller inherits ownership.
- **Liveness**: the owner writes a lease file (pid + heartbeat ticks + capacity) under the temp dir and refreshes
  it. `IsAlive` checks the lease is fresh *and* the pid is still running — so a crashed owner is distinguishable
  from a live one. Named `Mutex` and the temp-dir lease both work on Windows and Linux unchanged.

---

## Layer 3 — extensions

### Gossip

`GossipNode` is a replicated last-writer-wins key/value store. A local `Set` stamps a Lamport version and
**rumor-broadcasts** the entry on a lossy `ShmTopic` (O(1) on one machine). A periodic **anti-entropy** loop
re-gossips a random subset to repair lossy drops and bring late joiners current. Membership is a second presence
topic; a node is "discovered" once it has heard a peer. Conflicts resolve deterministically by
`(version, origin)`, so all nodes converge. Everything is lock-free over a `ConcurrentDictionary` + one
interlocked clock.

### Fusion

A Fusion-style attach/mirror/cache overlay (named for [ActualLab.Fusion](https://github.com/ActualLab/Fusion))
that demonstrates the layers composing: it uses **a topic for invalidations** and **RPC for fetches**.

```
ModelHost (origin)                          ModelMirror (consumer)
──────────────────                          ──────────────────────
Set(key, value):                            Get(key):
  store value, bump version                   if cached & consistent → return snapshot
  publish [version, key]  ──invalidate──►     else fetch over RPC, cache a SluiceComputed
  (NOT the value)                             listen loop: on invalidation → mark snapshot stale
serve OpGet over RPC  ◄──────fetch──────────  next Get refetches
```

The host signals *staleness*, never the value — minimal traffic, fetch-on-demand, exactly Fusion's
invalidate-then-recompute model. `SluiceComputed` is the immutable snapshot (value + version + consistency
state); `MirroredState` runs the read → await-invalidation → refetch loop as a push feed. The typed layer
(`ModelHost<T>` / `ModelMirror<T>` / `MirroredState<T>`) adds a `SluiceCodec<T>` serializer pair over the byte
substrate — the deserializer takes a `ReadOnlySpan<byte>` so it reads straight off the cached bytes, keeping the
typed face faithful to the zero-copy core.

---

## Correctness model (the short version)

- **Publication ordering** rests on paired release/acquire barriers: the producer's `Volatile.Write` of the
  cursor (ring) or slot sequence (multicast) *releases* the payload bytes written before it; the consumer's
  `Volatile.Read` *acquires* them. A consumer that observes the new cursor/stamp is guaranteed to observe the
  payload.
- **No torn reads under reliable delivery**: a slot/frame is not reused until the reader has advanced past it.
- **Torn reads under lossy delivery are detected**, not hidden: `TryReadCopy` re-validates the stamp after
  copying.
- **Crash robustness**: a dead RPC client can't crash the daemon (isolated dispatch); a crashed owner is
  reclaimable (`AbandonedMutexException` + stale lease); a dead lossy subscriber is simply lapped.
- **Single-machine only.** Cursors are raw pointers into shared pages — there is no network, no durability, and
  no cross-host story. Pair Sluice with a durable store if you need persistence.

See [delivery-and-safety](delivery-and-safety.md) for the failure-mode-by-failure-mode treatment.
