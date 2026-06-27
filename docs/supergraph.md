# Sluice.Supergraph — a federated reactive graph

`Sluice.Supergraph` is a **reactive graph that spans processes and hosts**. Vertices are owned by participants
spread across shared memory and the network; a change to any vertex propagates as a tiny invalidation across the
whole federation, so many processes behave as **one logical reactive graph**.

It is [Fusion](https://github.com/ActualLab/Fusion)'s model — *signal stale, recompute on demand* — generalized
over [`Sluice.Fabric`](federation.md) so it reaches beyond one machine. The same invalidate→refetch lifecycle
that `Sluice.Fusion` runs between co-located processes now runs across a federation: a co-located owner is reached
through shared memory (read in place, zero-copy), a remote owner over the network — and the calling code is
identical either way.

## The model

| Concept | What it is |
|---|---|
| **`GraphPeer`** | One per process. It owns some vertices and observes/computes over others, wherever they live. Together, peers form the supergraph. |
| **`VertexId`** | A global address: `(owner participant, key)`. The owner is the source of truth; everyone else mirrors it. |
| **`SourceVertex<T>`** | A vertex you own. `Set` updates it and invalidates every mirror across the federation. |
| **`ObservedVertex<T>`** | A self-refreshing view of any vertex. It raises `Updated` every time the owner changes it. |
| **`ComputedVertex<T>`** | A vertex you own that is *derived* from dependencies — which may be remote. It recomputes when any dependency changes, then publishes its own invalidation, so change propagates transitively. |
| **`Computed<T>`** | An immutable snapshot: the value, the owner's version, the `Reach` it was read from, and a consistency state. |

Three rules make it efficient:

- **A read of a vertex you own is a direct local read** — no round-trip, no copy out of your own store. The
  fabric was built to keep this fast path free.
- **A read of a vertex someone else owns** routes a correlated fetch to that owner. Co-located → shared memory;
  remote → the network. The result is cached and served until invalidated.
- **A change broadcasts only an invalidation** (key + version, never the value). Mirrors mark their copy stale and
  refetch lazily on next read. Minimal traffic; eventual freshness.

## A three-hop example

`prices` owns `spot`; `risk` computes `exposure = spot × size` (a dependency on `prices`); `desk` observes
`risk.exposure`. A change to `spot` ripples `prices → risk → desk` with no direct `prices ↔ desk` link.

```csharp
var codec = new SluiceCodec<double>(
    d => { var b = new byte[8]; BinaryPrimitives.WriteDoubleLittleEndian(b, d); return b; },
    s => BinaryPrimitives.ReadDoubleLittleEndian(s));

using var prices = new GraphPeer(new ShmTransport("prices"), "book");
using var risk   = new GraphPeer(new ShmTransport("risk"),   "book");
using var desk   = new GraphPeer(new ShmTransport("desk"),   "book");

var spot = prices.Define("spot", codec);

using var exposure = risk.Computed("exposure", codec,
    () => risk.Get(spot.Id, codec).Value * 100.0,   // reads a remote dependency
    spot.Id);

using var view = desk.Observe(exposure.Id, codec);
view.Updated += v => Console.WriteLine($"exposure = {v:N2}");

spot.Set(43.25);   // ripples prices → risk → desk
```

See [`samples/Sluice.SupergraphDemo`](../samples/Sluice.SupergraphDemo) for the runnable version.

## Federation: local and remote in one graph

A `GraphPeer` takes any `ITransport`. Give it a [`FederatedTransport`](federation.md) and the graph spans hosts:
co-located participants stay zero-copy in shared memory, and participants on the other end of a socket join the
same graph — invalidations and fetches cross the boundary transparently.

```csharp
var fed = new FederatedTransport(
    new ShmTransport("alice"),                                    // co-located participants, zero-copy
    new TcpTransport(new ParticipantId("node:alice"), 9000,       // remote participants, over the network
        (new ParticipantId("node:bob"), "10.0.0.2", 9000)));
using var peer = new GraphPeer(fed, "book");
```

**Owner addresses are reach-relative.** A vertex owned by a node you reach over the network is addressed by that
node's network id (`node:alice`); the same vertex observed by a co-located participant is addressed by the owner's
shared-memory id. Use the owner id as you observe it — it is exactly the `From` you receive that vertex's
invalidations under, and the address its fetch routes back to. (A presence directory that unifies the two into one
logical id is a planned upgrade; the routing is correct either way today.)

## Robustness

A federated graph must tolerate a dropped or delayed message — over a network, some always are. The reactive loops
are built to self-heal rather than wedge:

- **Reads happen off the channel pump.** An `ObservedVertex` / `ComputedVertex` runs its fetch on its own thread;
  the pump only signals it. A blocking remote fetch can never stall message delivery.
- **A lost fetch retries.** If a fetch gets no reply (a dropped request, an owner not yet reachable), the loop
  retries shortly instead of treating the miss as settled — so a transient loss heals on its own.
- **An invalidation is never forgotten.** Each mirror tracks the highest version it has seen invalidated for a
  vertex, *even before it holds a value*. A fetched reply older than that high-water mark is recognized as stale on
  arrival and retried. This closes the race where an invalidation arrives before the first cache fill.
- **A throwing handler can't deafen a channel.** The transport pump isolates each delivery, so one bad handler
  call drops only its own message, never the whole pump.

These are verified by a convergence stress test that runs the three-hop chain repeatedly, including under
concurrent load (the conditions that starve background threads).

## Status

Built and tested: the seam, source/observed/computed vertices, the cross-participant and federated (shared-memory +
TCP) reactive paths, and the robustness behaviors above.

Planned, additive: a presence directory that unifies a federated node's per-reach addresses into one logical id;
richer membership (a gossip-backed roster beyond the channel's observed view); and a delta/streaming push for
large values where refetch-the-whole-thing is wasteful.

## Relationship to the other Sluice layers

- [`Sluice.Fusion`](README.md) is the same invalidate→refetch model **between co-located processes**.
  `Sluice.Supergraph` is that model **federated** — it reuses Fusion's `SluiceCodec<T>` and `ConsistencyState`.
- [`Sluice.Fabric`](federation.md) is the transport seam underneath — `IChannel` / `ITransport` / `FederatedTransport`.
- [`Sluice.Gossip`](README.md) is the membership/anti-entropy layer that a richer roster will build on.
