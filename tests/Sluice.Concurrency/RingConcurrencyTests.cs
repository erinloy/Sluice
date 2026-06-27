using Microsoft.Coyote.Specifications;
using Microsoft.Coyote.SystematicTesting;
using Sluice;

namespace Sluice.Concurrency;

/// <summary>
/// Systematic-concurrency tests for the lock-free rings, driven by Microsoft Coyote.
///
/// <para>
/// Unit tests run a ring once, on whatever interleaving the OS happens to pick. Coyote instead takes control
/// of the scheduler and explores the interleavings *exhaustively* (within a step bound) over thousands of
/// iterations — every order in which the producer's release of the write cursor can race the consumer's
/// acquire of it. If any schedule loses, duplicates, reorders, or tears a message, Coyote reports it with a
/// replayable trace. These tests assert the ring's core safety property holds under <i>all</i> of them.
/// </para>
///
/// <para>
/// The capacities are deliberately tiny so the ring wraps several times (exercising the skip-marker filler
/// and the cursor modular arithmetic — the trickiest part of the protocol) within a bounded message count.
/// The doorbell is disabled: it is a kernel <c>EventWaitHandle</c> Coyote cannot model, and it is purely a
/// latency optimisation — the ring is correct by polling, which is exactly what we want to verify.
/// </para>
///
/// <para>
/// Run via <c>scripts/run-coyote.ps1</c> (or <c>.sh</c>), which rewrites the assemblies and invokes
/// <c>coyote test</c>. Without rewriting, the scheduler is not in control and these would be ordinary
/// (much weaker) stress runs.
/// </para>
/// </summary>
public static class RingConcurrencyTests
{
    // Per-invocation unique map name. Interlocked is scheduler-controlled, so this stays deterministic under
    // Coyote (no Guid/clock nondeterminism leaking into the explored state).
    private static int _seq;

    private static string NextName(string prefix) => $"{prefix}-{Interlocked.Increment(ref _seq)}";

    /// <summary>
    /// One producer, one consumer, a ring far too small to hold the stream: the producer must backpressure on
    /// the consumer and the ring must wrap repeatedly. Across every interleaving Coyote explores, the consumer
    /// must observe exactly the published sequence 0..N-1 — in order, none lost, none duplicated, none torn.
    /// </summary>
    [Test]
    public static void Spsc_ring_never_loses_reorders_or_tears_a_message()
    {
        const long capacity = 64;   // data region; an 8-byte frame fits 8× before a wrap
        const int n = 24;           // ≫ ring depth → several wraps + skip markers per run

        using var ring = ShmRing.Create(NextName("coyote-ring"), capacity, withDoorbell: false);

        var consumed = new List<int>(n);
        var consumer = Task.Run(() =>
        {
            int got = 0;
            while (got < n)
            {
                if (ring.TryRead(out var span))
                {
                    consumed.Add(BitConverter.ToInt32(span));   // read in place, before AdvanceRead frees the slot
                    ring.AdvanceRead();
                    got++;
                }
            }
        });

        var producer = Task.Run(() =>
        {
            for (int i = 0; i < n; i++)
                ring.Write(BitConverter.GetBytes(i));
        });

        Task.WaitAll(producer, consumer);

        Specification.Assert(consumed.Count == n,
            $"consumer saw {consumed.Count} messages, expected {n} (loss or duplication)");
        for (int i = 0; i < n; i++)
            Specification.Assert(consumed[i] == i,
                $"message {i} arrived as {consumed[i]} (reorder, tear, or stale read)");
    }

    /// <summary>
    /// The read/advance handshake under contention: the consumer reads a span <i>in place</i> and only frees
    /// the slot on <see cref="ShmRing.AdvanceRead"/>. This verifies the producer can never overwrite a slot the
    /// consumer is still reading — the property that makes Sluice's zero-copy read sound. We capture the value
    /// before advancing and assert it survived every interleaving where the producer was poised to reclaim it.
    /// </summary>
    [Test]
    public static void Read_in_place_slot_is_not_reclaimed_before_AdvanceRead()
    {
        const long capacity = 32;   // only ~4 frames live at once → producer is constantly poised to reclaim
        const int n = 16;

        using var ring = ShmRing.Create(NextName("coyote-place"), capacity, withDoorbell: false);

        var consumer = Task.Run(() =>
        {
            int got = 0;
            while (got < n)
            {
                if (ring.TryRead(out var span))
                {
                    int seen = BitConverter.ToInt32(span);          // value read out of the live slot
                    Thread.Yield();                                 // a scheduling point WHILE holding the span
                    int again = BitConverter.ToInt32(span);         // same slot, after any producer activity
                    Specification.Assert(seen == again && seen == got,
                        $"slot mutated under an in-place read: first {seen}, then {again}, expected {got}");
                    ring.AdvanceRead();
                    got++;
                }
            }
        });

        var producer = Task.Run(() =>
        {
            for (int i = 0; i < n; i++)
                ring.Write(BitConverter.GetBytes(i));
        });

        Task.WaitAll(producer, consumer);
    }
}
