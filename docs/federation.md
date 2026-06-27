# The fabric — one channel, local and remote

Sluice's topologies all assume every participant is on the same machine, sharing memory. The **fabric** lifts
that assumption behind a transport-agnostic seam, so a single channel can hold participants that share memory
locally *and* participants reached across a network — without the calling code knowing or caring which is which.
A co-located participant stays zero-copy in shared memory; a remote one is reached over a socket; the API is the
same either way.

This is the layer an RPC system or a sharding/federation layer sits on top of when some of its peers are local
and some are not.

## The seam

Four small contracts in `Sluice.Fabric`:

- **`ParticipantId`** — a transport-agnostic address. Nothing parses it; a transport simply declares which ids
  it `Owns`. Sluice's transports use `"shm:…"`-style ids for same-host participants and `"node:…"` for remote
  ones, but routing is by ownership, not by string-matching.
- **`IChannel`** — a named medium: `Broadcast` to everyone (1→N), an addressed `Send` to one peer (→1),
  `Subscribe` to receive, and a live `Participants` view. It does not know whether a participant is local or
  remote.
- **`ISignal`** — the signaling seam: `Notify()` / `Wait(ct)`. A local channel backs it with the shared-memory
  doorbell (or its bounded polling fallback); a remote channel backs it with network readiness. It is the
  doorbell, generalized so it can be either.
- **`ITransport`** — realizes channels over one medium, and declares which participants it `Owns`.

A message arrives as an `Inbound`: the sender's `ParticipantId`, an `int Kind`, and a `ReadOnlySpan<byte>`
payload. Local delivery hands that span straight from shared memory (no copy); remote delivery hands it over a
network buffer. Same shape — that is the point.

```csharp
public delegate void ChannelHandler(in Inbound message);   // span valid for the call only; copy what you keep
```

## The three transports

| Transport            | Reach   | How                                                                |
|----------------------|---------|-------------------------------------------------------------------|
| `ShmTransport`       | Local   | composes `ShmTopic` (broadcast) + `ShmPeer` (addressed), zero-copy |
| `TcpTransport`       | Remote  | channels over TCP — length-prefixed frames, multiplexed per channel |
| `FederatedTransport` | Both    | composes a local + N remote transports into one channel           |

### Local — `ShmTransport`

```csharp
using var alice = new ShmTransport("alice");
using var bob   = new ShmTransport("bob");
using var ca = alice.Open("orders");
using var cb = bob.Open("orders");

using var sub = cb.Subscribe((in Inbound m) =>
    Console.WriteLine($"{m.From} #{m.Kind}: {Encoding.UTF8.GetString(m.Payload)}"));

ca.Broadcast(kind: 1, "fill"u8);                            // to every participant on "orders"
ca.Send(new ParticipantId("fabric.orders.bob"), kind: 2, "ack"u8);   // addressed to bob
```

### Remote — `TcpTransport`

```csharp
// node A listens on 9001 and dials node B at 9002
using var a = new TcpTransport(new ParticipantId("node:a"), 9001,
    (new ParticipantId("node:b"), "10.0.0.2", 9002));
using var ca = a.Open("orders");
ca.Broadcast(1, "fill"u8);                                  // reaches every connected node on "orders"
```

### Both — `FederatedTransport`

The reason the fabric exists: one channel, both reaches. Broadcasting fans out to same-host participants through
shared memory and to remote nodes over the network in the same call; an addressed send routes to whichever
transport owns the target. With no remote member supplied it degrades exactly to its local member — adopting the
fabric never costs you the local fast path.

```csharp
using var fabric = new FederatedTransport(
    new ShmTransport("alice"),                                          // co-located peers, zero-copy
    new TcpTransport(new ParticipantId("node:a"), 9001,                 // everyone else, over TCP
        (new ParticipantId("node:b"), "10.0.0.2", 9002)));

using var channel = fabric.Open("orders");
channel.Broadcast(1, "fill"u8);   // one call → local shm participants AND remote node B both receive it
```

## Status & limits

- **`TcpTransport`** is a clean, dependency-free socket transport: length-prefixed self-describing frames, one
  connection multiplexing every shared channel, hello-based peer identification so addressed sends find the
  right socket. It does **not** yet do TLS or automatic reconnection — a dropped connection stays dropped. Both
  are additive follow-ons behind the same `ITransport`.
- **Membership** (`IChannel.Participants`) is observed: locally, the peers you have heard from; remotely, the
  connected nodes. A presence-beacon roster is a planned upgrade.
- **Delivery** follows the underlying transport: the shared-memory path offers `Reliable`/`Lossy` as everywhere
  else; the TCP path is an ordered stream per connection.

See [architecture](architecture.md) for how the local transport composes the shared-memory primitives, and
[topologies](topologies.md) for the same-host shapes the fabric generalizes.
