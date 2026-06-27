# Delivery semantics & safety

What each delivery mode guarantees, how torn reads are handled, and what happens when a participant crashes.

## Reliable vs lossy

Multicast rings (`ShmTopic`, `ShmPeer`, and the gossip topics) carry a `DeliveryMode` chosen by whoever creates
the ring. Point-to-point RPC and frame channels are always reliable.

### Reliable

> No message is ever dropped. A producer waits until the slowest registered subscriber has consumed past the
> slot it wants to reuse before overwriting it.

- **Guarantee**: every subscriber that is registered when a message is published will see that message.
- **Cost**: a stalled or slow subscriber **backpressures every producer** on the ring — the ring is only as fast
  as its slowest reader. A subscriber that registers and then stops reading (without disposing) will eventually
  freeze all publishers.
- **Use when**: consumers are cooperative and in-process-family (you control them), and completeness matters more
  than isolation — commands, events you must not lose, the [Fusion](https://github.com/ActualLab/Fusion) invalidation channel.

### Lossy

> A producer never waits on consumers. It overwrites the oldest slot. A subscriber that falls more than a ring
> behind detects the gap and resyncs forward, reporting how many it missed.

- **Guarantee**: a subscriber always sees a *contiguous, current* suffix of the stream; if it can't keep up it
  skips forward to the oldest still-live message rather than blocking the producer. `Subscriber.Dropped` counts
  exactly how many it skipped.
- **Cost**: messages can be missed by a slow reader. That's the trade, by design.
- **Use when**: latency and isolation matter more than completeness, and a stale message is worthless anyway —
  market data, telemetry, presence/heartbeats, gossip rumors.
- **Crash-robust**: a subscriber that dies is simply lapped. It never backpressures or endangers producers, so
  lossy rings tolerate consumer crashes by construction.

## Torn reads

A torn read is only possible under **lossy** delivery, and only on the in-place path: a fast producer can
overwrite a slot *while* a reader is reading the span that points into it.

Sluice gives you two reads:

| Method                         | Copy? | Torn-read safe?                          | Use for                                    |
|--------------------------------|-------|------------------------------------------|--------------------------------------------|
| `TryRead(out ReadOnlySpan)`    | no    | safe under **reliable**; can tear under **lossy** | reliable rings, or lossy when you detect tears yourself |
| `TryReadCopy(out byte[])`      | yes   | always safe                              | lossy rings, the default convenience path  |

`TryReadCopy` copies the payload, then **re-validates the slot's sequence stamp**. If the slot was clobbered
mid-copy, the copy is discarded, the gap is counted in `Dropped`, and the read retries at the resynced position.
This is why `ShmPeer.Receive`, gossip, and any lossy `ShmTopic` consumer that wants safety use `TryReadCopy`.

Under **reliable** delivery the in-place `TryRead` is always safe — the slot can't be reused until you advance —
so you get true zero-copy reads with no caveat. This is the same guarantee the SPSC `ShmRing` gives RPC and the
frame channel unconditionally (they are point-to-point and reliable).

### The read-in-place contract

For the zero-copy paths, the span you receive points **into shared memory** and is valid only until you advance:

```csharp
if (sub.TryRead(out var span))   // or ring.TryRead / channel.TryReadFrame
{
    Use(span);                   // valid now
    sub.Advance();               // span is INVALID after this — the slot may be reused
}
```

Never retain the span past `Advance()` / `AdvanceRead()` / `AdvanceFrame()`. Copy out anything you need to keep.

## Memory ordering

Cross-process visibility rests on paired release/acquire barriers, not on locks:

- A producer writes the payload bytes, then does a `Volatile.Write` of the cursor (`ShmRing`) or the slot's
  sequence stamp (`ShmMulticast`). That write is a **release** — every store before it is visible to any thread
  that observes the new cursor/stamp.
- A consumer does a `Volatile.Read` of that cursor/stamp before touching the payload. That read is an
  **acquire** — if it observes the new value, it observes the payload writes that preceded the release.

So a consumer that sees "there is a new message" is guaranteed to see the *whole* message. The per-slot stamp in
multicast is the publish barrier; the monotonic cursors in the SPSC ring are. No `lock`, no kernel transition on
the steady-state path.

## Crash robustness

| Who crashes              | What happens                                                                                  |
|--------------------------|-----------------------------------------------------------------------------------------------|
| An RPC **client**, mid-call | The daemon isolates each dispatch (`try`/`catch` → `OnError`); a vanished client (e.g. its response ring is gone) can't take the daemon down. |
| The RPC **owner/daemon** | Its named `Mutex` surfaces `AbandonedMutexException` to the next `TryBecomeOwner` → ownership is *inherited*, the endpoint reclaimable. The stale lease (dead pid / old heartbeat) makes `IsAlive` return false so clients know to (re)start it. |
| A **lossy** subscriber   | Simply lapped. No backpressure, no producer impact.                                            |
| A **reliable** subscriber that stalls | Backpressures producers (by design). Dispose subscribers you no longer read, or use lossy. |
| A process holding a Linux backing file | tmpfs is volatile — reclaimed on container restart; a new owner overwrites. See [deployment](deployment.md#operational-notes). |

## What Sluice does *not* protect against

- **It is not durable.** Everything lives in RAM (or tmpfs). A reboot, a container restart, or all-handles-closed
  loses the data. Pair with a log/store if you need persistence.
- **It is not a security boundary.** Any process that can open the named region or backing file can read and
  write it. Use OS permissions on the `/dev/shm` path / named objects to scope access.
- **It is not cross-host.** Cursors are pointers into shared pages; there is no network layer.
- **It does not validate payloads.** The bytes are whatever the producer wrote. If you read a blittable struct in
  place from an untrusted producer, you are trusting that producer's layout. Within a cooperative process family
  (the design point) this is exactly what you want; across a trust boundary, validate.

## Bounded waits & cancellation

Every blocking call takes a `CancellationToken` and uses *bounded* internal waits, so cancellation is always
observed even if a signal is missed:

- `ShmRing.WaitToRead` — spin, then a 20 ms-bounded doorbell wait (Windows) or a ≤ 1 ms polling backoff (Linux),
  re-checking the token each loop.
- `ShmMulticast.Subscriber.Wait` — spin, then a 1 ms-capped idle sleep, re-checking the token. (No doorbell:
  multicast can't be correctly woken by a single shared event — see
  [architecture](architecture.md#layer-1b--shmmulticast-the-sequenced-mpmc-ring).)

This means a missed wakeup degrades to a slightly later wakeup, never a hang.

## Systematic concurrency testing

The ring's correctness rests on one release/acquire pairing: the producer writes the payload bytes and *then*
releases the write cursor; the consumer acquires the write cursor and *then* reads the bytes. A unit test runs
that protocol once, on whatever interleaving the OS happened to pick — it can't tell you the protocol is right,
only that it didn't break *this time*.

[Microsoft Coyote](https://github.com/microsoft/coyote) closes that gap. It rewrites the assemblies, takes
control of the scheduler, and explores the producer/consumer interleavings *exhaustively* within a step bound —
thousands of distinct schedules, each making thousands of scheduling decisions at every cursor read, spin, and
task yield. The tests in `tests/Sluice.Concurrency` assert the ring's safety property across all of them:

- **`Spsc_ring_never_loses_reorders_or_tears_a_message`** — a stream far larger than a deliberately tiny ring
  (forcing repeated wraps and skip-marker fillers); the consumer must observe exactly `0..N-1`, in order, with
  no loss, duplication, or torn read, under every schedule.
- **`Read_in_place_slot_is_not_reclaimed_before_AdvanceRead`** — the zero-copy guarantee: a slot a consumer is
  reading in place is never overwritten by the producer before `AdvanceRead`, even with the producer poised to
  reclaim it at every scheduling point.

Both explore cleanly with **0 bugs**. Run them with [`scripts/run-coyote.ps1`](../scripts/run-coyote.ps1) (or
`.sh`); CI runs them on every push as the `concurrency-check` job. The harness is verified to *find* bugs — an
injected lost-update race is caught in 100% of schedules — so a green run is a real proof of safety, not a test
that merely can't fail.
