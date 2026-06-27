using System.Text;
using Sluice;
using Xunit;

namespace Sluice.Tests;

public class ShmRingTests
{
    private static string N() => "test." + Guid.NewGuid().ToString("N");

    [Fact]
    public void Write_then_read_round_trips_payload()
    {
        using var ring = ShmRing.Create(N(), 4096, withDoorbell: false);
        var msg = Encoding.UTF8.GetBytes("hello sluice");

        Assert.True(ring.TryWrite(msg));
        Assert.True(ring.TryRead(out var span));
        Assert.Equal(msg, span.ToArray());
        ring.AdvanceRead();

        Assert.True(ring.IsEmpty);
        Assert.False(ring.TryRead(out _));
    }

    [Fact]
    public void Read_sees_bytes_written_through_a_separate_mapping()
    {
        var name = N();
        using var owner = ShmRing.Create(name, 4096, withDoorbell: false);
        using var opened = ShmRing.Open(name, withDoorbell: false);

        var msg = Encoding.UTF8.GetBytes("cross-mapping");
        Assert.True(owner.TryWrite(msg));          // write through one mapping
        Assert.True(opened.TryRead(out var span)); // read through another
        Assert.Equal(msg, span.ToArray());
        opened.AdvanceRead();
        Assert.Equal(4096 - 8, opened.MaxPayload);
    }

    [Fact]
    public void Many_messages_survive_repeated_wrap_around()
    {
        using var ring = ShmRing.Create(N(), 256, withDoorbell: false); // tiny ring forces many wraps
        for (int i = 0; i < 5000; i++)
        {
            var payload = Encoding.UTF8.GetBytes($"msg-{i}-{new string('x', i % 40)}");
            Assert.True(ring.TryWrite(payload));
            Assert.True(ring.TryRead(out var span));
            Assert.Equal(payload, span.ToArray());
            ring.AdvanceRead();
        }
        Assert.True(ring.IsEmpty);
    }

    [Fact]
    public void TryWrite_returns_false_when_full_then_succeeds_after_drain()
    {
        using var ring = ShmRing.Create(N(), 64, withDoorbell: false);
        var chunk = new byte[20];

        int written = 0;
        while (ring.TryWrite(chunk)) written++;
        Assert.True(written >= 1);
        Assert.False(ring.TryWrite(chunk));   // genuinely full — no exception, just false

        Assert.True(ring.TryRead(out _));     // free one slot
        ring.AdvanceRead();
        Assert.True(ring.TryWrite(chunk));    // now there is room
    }

    [Fact]
    public void Oversized_payload_throws()
    {
        using var ring = ShmRing.Create(N(), 64, withDoorbell: false);
        Assert.Throws<ArgumentException>(() => ring.TryWrite(new byte[ring.MaxPayload + 1]));
    }

    [Fact]
    public async Task Producer_and_consumer_threads_transfer_every_message_in_order()
    {
        var name = N();
        using var producer = ShmRing.Create(name, 8192);
        using var consumer = ShmRing.Open(name);

        const int count = 100_000;
        var producerTask = Task.Run(() =>
        {
            var b = new byte[4];
            for (int i = 0; i < count; i++)
            {
                BitConverter.TryWriteBytes(b, i);
                producer.Write(b);
            }
        });

        int next = 0;
        while (next < count)
        {
            consumer.WaitToRead();
            while (consumer.TryRead(out var span))
            {
                Assert.Equal(next, BitConverter.ToInt32(span));
                next++;
                consumer.AdvanceRead();
            }
        }

        await producerTask;
        Assert.Equal(count, next);
    }
}
