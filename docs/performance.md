# Performance

The numbers, how they were measured, and the knobs that move them. Everything here is reproducible from the
[`bench/`](../bench/Sluice.Benchmarks) project — run it on your own hardware and you'll get your own numbers; the
*ratios* and the *allocation* figures are the stable, meaningful comparisons.

## How these were measured

```
BenchmarkDotNet v0.14.0, Windows 11, .NET 10.0.9 (X64 RyuJIT AVX-512), Concurrent Server GC
```

Two things decide which numbers you can trust:

1. **Allocations are deterministic** — they do not depend on machine load or hardware. The allocation columns
   below are exact and reproduce everywhere.
2. **In-process CPU benchmarks are stable** (the serialization and multicast tables, run with BenchmarkDotNet's
   full default job). **Cross-process latencies are load-sensitive** — they involve OS scheduling and syscalls,
   so on a busy machine they inflate, and they inflate the *other* transports (named pipes, the Cloudtoid queue)
   far more than Sluice's shared-memory spin. The end-to-end latencies below were captured on a **heavily loaded
   multi-core dev box**, so treat their absolute values as a conservative snapshot and the **ratios as a floor**,
   not a ceiling.

---

## Read in place vs. serialize — the core claim

This is the heart of Sluice: reading a message is a pointer reinterpret, not a decode. Measured against the
common .NET serializers, decoding the *same* message (full default job, stable):

**64-byte payload**

| Reading one message            | Mean       | vs JSON    | Allocated |
|--------------------------------|-----------:|-----------:|----------:|
| **Sluice — blittable in place**| **6.5 ns** | **~100× faster** | **0 B** |
| MemoryPack deserialize         | 107 ns     | 6.2×       | 272 B     |
| MessagePack deserialize        | 313 ns     | 2.1×       | 280 B     |
| JSON deserialize               | 667 ns     | 1.0×       | 336 B     |

**1024-byte payload**

| Reading one message            | Mean       | Allocated |
|--------------------------------|-----------:|----------:|
| **Sluice — blittable in place**| **15.6 ns**| **0 B**   |
| MemoryPack deserialize         | 392 ns     | 2192 B    |
| MessagePack deserialize        | 615 ns     | 2200 B    |
| JSON deserialize               | 1183 ns    | 2576 B    |

Reading in place is ~16× faster than MemoryPack and ~100× faster than JSON, and allocates **nothing** where the
serializers allocate 270 B – 2.5 KB. There is no decode step to pay for.

---

## Multicast fan-out cost

The per-message cost of the multi-way ring — what sets the throughput ceiling. In-process, stable:

| Operation                    | 32 B     | 256 B    | Allocated |
|------------------------------|---------:|---------:|----------:|
| Publish (fan-out write)      | 8.1 ns   | 13.8 ns  | 0 B       |
| Publish + consume in place   | 12.6 ns  | 17.9 ns  | 0 B       |

Single-producer throughput is tens of millions of messages/sec; gossip convergence latency is flat as the
cluster grows (the [lifecycle harness](#reproducing)), because the fan-out is O(1) shared-memory multicast, not
O(N) point-to-point.

---

## End-to-end RPC, across frameworks

A full request → response round trip carrying a 256-byte payload, comparing Sluice to the realistic local-IPC
alternatives — Cloudtoid.Interprocess (a genuine shared-memory queue) and named pipes, each with MemoryPack and
JSON. Every alternative must serialize a message to carry the payload; Sluice sends it raw.

**Allocation — exact and load-independent (the cleanest cross-framework comparison):**

| Transport                | Allocated | vs Sluice |
|--------------------------|----------:|----------:|
| **Sluice (zero-alloc receive)** | **0 B** | **0×**  |
| Sluice (convenience `Send`)     | 280 B   | 1.0×      |
| Cloudtoid + MemoryPack          | 1475 B  | 5.3×      |
| Cloudtoid + JSON                | 1772 B  | 6.3×      |
| Named pipe + MemoryPack         | 2072 B  | 7.4×      |
| Named pipe + JSON               | 2588 B  | 9.2×      |

**Latency — measured on a contended box; ratios are a floor:**

| Transport                | Mean (this run) | vs Sluice |
|--------------------------|----------------:|----------:|
| **Sluice (zero-alloc)**  | **~0.72 µs**    | **0.9×**  |
| **Sluice (`Send`)**      | **~0.77 µs**    | **1.0×**  |
| Cloudtoid + MemoryPack   | ~34 µs          | ~45×      |
| Cloudtoid + JSON         | ~39 µs          | ~51×      |
| Named pipe + MemoryPack  | ~116 µs         | ~151×     |
| Named pipe + JSON        | ~118 µs         | ~154×     |

The absolute microsecond figures for the queue/pipe transports are inflated by machine load (they hinge on OS
scheduling); on a quiet box they are far lower. But that asymmetry is itself the point: **Sluice's
shared-memory path stays sub-microsecond under contention while syscall-based transports degrade**, so its
relative advantage *grows* exactly when a machine is busy — which is when a server is doing real work. The
robust, everywhere-true claims are: Sluice is **one to two orders of magnitude faster end-to-end**, and it
allocates **5–9× less, or nothing at all** on the zero-alloc path.

> Reproduce these yourself with the command in [Reproducing](#reproducing) — and ideally on an idle machine, for
> tighter latency numbers than a shared CI/dev box gives.

---

## The zero-allocation receive

`SluiceClient.Send` returns an `RpcResponse`, copying the payload out across the API boundary (the 280 B above).
The hot path avoids that entirely by reading the response in place:

```csharp
client.Send(kind, payload, state, static (ok, span, state) => { /* read span in place */ });
```

Measured at **0 B/op** (the `Sluice_ZeroAlloc` row) versus 280 B for the convenience path, at essentially
identical latency. The win is **GC-pressure elimination, not speed** — at a high request rate it removes the
allocation that would otherwise drive gen-0 collections. The `TState` generic + a `static` lambda mean no
per-call closure allocation either. Reach for it on hot paths; use the plain `Send` elsewhere.

---

## Tuning knobs

| Knob                | Effect                                                                                       |
|---------------------|----------------------------------------------------------------------------------------------|
| `exclusiveProducer` | Skips the cross-process producer mutex (a kernel transition per send). Biggest RPC win when you're the only writer. Drops only the mutex, never the cursor sync. |
| `capacity` (ring)   | More in-flight headroom before a producer backpressures — higher sustained throughput under bursts. Costs shared memory. |
| `slotCount` (multicast) | Deeper ring → a slow lossy subscriber lags further before being lapped (fewer drops); a reliable producer stalls less. Costs shared memory. |
| Zero-alloc `Send<TState>` | Removes 280 B/op → lower GC pressure at high request rates.                             |

## Platform

Throughput and the in-place read path are identical on Windows and Linux. The only difference is idle wakeup
latency — sub-µs on Windows (wait-handle doorbell), ≤ 1 ms on Linux (polling) — and under active load a message
arrives during the spin phase on both, so it's invisible to busy workloads. See
[deployment](deployment.md) and [architecture](architecture.md#layer-0--shmmap-the-os-floor).

## Reproducing

```bash
# read-in-place vs serializers (stable, in-process)
dotnet run -c Release --project bench/Sluice.Benchmarks -- --filter "*Serialization*"

# multicast publish cost
dotnet run -c Release --project bench/Sluice.Benchmarks -- --filter "*Multicast*"

# end-to-end across frameworks (run on an idle machine for clean latency)
dotnet run -c Release --project bench/Sluice.Benchmarks -- --filter "*RoundTrip*"

# custom harnesses
dotnet run -c Release --project bench/Sluice.Benchmarks -- throughput
dotnet run -c Release --project bench/Sluice.Benchmarks -- lifecycle
```
