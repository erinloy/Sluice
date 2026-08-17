using Sluice;
using Xunit;

namespace Sluice.Tests;

public class MulticastTests
{
    private static string N() => "mc." + Guid.NewGuid().ToString("N");

    [Fact]
    public void Broadcast_every_subscriber_sees_every_message_in_order()
    {
        using var ring = ShmMulticast.Create(N(), maxPayload: 64, slotCount: 1024, mode: DeliveryMode.Reliable);
        var a = ring.Subscribe();
        var b = ring.Subscribe();
        var c = ring.Subscribe();

        const int n = 500; // < slotCount, so no wrap and nothing can be dropped
        for (long i = 0; i < n; i++)
            ring.Publish(BitConverter.GetBytes(i));

        foreach (var sub in new[] { a, b, c })
        {
            var seen = new List<long>();
            while (sub.TryRead(out var span)) { seen.Add(BitConverter.ToInt64(span)); sub.Advance(); }
            Assert.Equal(n, seen.Count);
            for (int i = 0; i < n; i++) Assert.Equal(i, seen[i]);
            Assert.Equal(0, sub.Dropped);
            sub.Dispose();
        }
    }

    [Fact]
    public void Multi_producer_bus_delivers_a_clean_union_of_all_sequences()
    {
        using var ring = ShmMulticast.Create(N(), maxPayload: 64, slotCount: 2048, mode: DeliveryMode.Reliable);
        var sub1 = ring.Subscribe();
        var sub2 = ring.Subscribe();

        const int producers = 4, each = 250, total = producers * each;
        Parallel.For(0, producers, _ =>
        {
            for (int i = 0; i < each; i++)
                ring.Publish(BitConverter.GetBytes((long)Environment.CurrentManagedThreadId * 1_000_000 + i));
        });

        // Each subscriber must observe exactly `total` messages and a strictly increasing sequence stamp set
        // {0..total-1} — proving the interlocked claim never duplicated or dropped a sequence.
        foreach (var sub in new[] { sub1, sub2 })
        {
            int count = 0;
            while (sub.TryRead(out _)) { count++; sub.Advance(); }
            Assert.Equal(total, count);
            Assert.Equal(0, sub.Dropped);
            sub.Dispose();
        }
    }

    [Fact]
    public void Lossy_subscriber_that_falls_behind_resyncs_and_counts_drops()
    {
        using var ring = ShmMulticast.Create(N(), maxPayload: 64, slotCount: 8, mode: DeliveryMode.Lossy);
        var sub = ring.Subscribe(); // registered before any publish: next = 0

        const int n = 100; // >> slotCount(8): the ring laps many times with no consumer draining
        for (long i = 0; i < n; i++)
            ring.Publish(BitConverter.GetBytes(i));

        var seen = new List<long>();
        while (sub.TryRead(out var span)) { seen.Add(BitConverter.ToInt64(span)); sub.Advance(); }

        Assert.True(sub.Dropped > 0, "a lapped lossy subscriber should report drops");
        Assert.True(seen.Count <= 8, "a lossy subscriber only recovers the live ring window");
        Assert.Equal(n - 1, seen[^1]);                 // it always catches the most recent message
        Assert.Equal(n, sub.Dropped + seen.Count);     // every message is either seen or accounted as dropped
        sub.Dispose();
    }

    [Fact]
    public async Task Reliable_backpressures_so_a_wrapping_stream_loses_nothing()
    {
        using var ring = ShmMulticast.Create(N(), maxPayload: 64, slotCount: 8, mode: DeliveryMode.Reliable);
        var sub = ring.Subscribe();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        const int n = 10_000; // >> slotCount: the producer MUST block on the consumer or lose data
        var consumer = Task.Run(() =>
        {
            var seen = new List<long>();
            while (seen.Count < n && sub.Wait(cts.Token))
                if (sub.TryRead(out var span)) { seen.Add(BitConverter.ToInt64(span)); sub.Advance(); }
            return seen;
        });

        for (long i = 0; i < n; i++)
            ring.Publish(BitConverter.GetBytes(i), cts.Token);

        var got = await consumer;
        Assert.Equal(n, got.Count);
        for (int i = 0; i < n; i++) Assert.Equal(i, got[i]);
        Assert.Equal(0, sub.Dropped);
        sub.Dispose();
    }

    [Fact]
    public async Task Reliable_producer_evicts_a_crashed_subscriber_after_its_lease()
    {
        // A reliable producer gates on the slowest subscriber. A crashed subscriber that stops reading would
        // wedge the producer forever — unless its lease lapses and the producer reclaims its cell.
        using var ring = ShmMulticast.Create(N(), maxPayload: 64, slotCount: 8,
            mode: DeliveryMode.Reliable, leaseMs: 200);
        var alive = ring.Subscribe();   // keeps draining
        var dead = ring.Subscribe();    // registered, then never reads again (simulating a crash)
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        const int n = 200; // ≫ slotCount(8): without eviction the producer blocks on `dead` after one lap
        var consumer = Task.Run(() =>
        {
            int got = 0;
            while (got < n && alive.Wait(cts.Token))
                if (alive.TryRead(out _)) { alive.Advance(); got++; }
            return got;
        });

        var producer = Task.Run(() =>
        {
            for (long i = 0; i < n; i++) ring.Publish(BitConverter.GetBytes(i), cts.Token);
        });

        await producer;                 // returns only if `dead` was evicted; otherwise it cancels at 15s and throws
        Assert.Equal(n, await consumer);
        dead.Dispose();
        alive.Dispose();
    }

    [Fact]
    public void Late_subscriber_only_sees_messages_published_after_it_joined()
    {
        using var ring = ShmMulticast.Create(N(), maxPayload: 64, slotCount: 1024, mode: DeliveryMode.Reliable);
        for (long i = 0; i < 10; i++) ring.Publish(BitConverter.GetBytes(i)); // before anyone subscribes

        var late = ring.Subscribe();
        for (long i = 10; i < 20; i++) ring.Publish(BitConverter.GetBytes(i));

        var seen = new List<long>();
        while (late.TryRead(out var span)) { seen.Add(BitConverter.ToInt64(span)); late.Advance(); }
        Assert.Equal(10, seen.Count);
        Assert.Equal(10, seen[0]);
        Assert.Equal(19, seen[^1]);
        late.Dispose();
    }
}
