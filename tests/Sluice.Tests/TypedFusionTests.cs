using MemoryPack;
using Sluice.Fusion;
using Xunit;

namespace Sluice.Tests;

public class TypedFusionTests
{
    private static string Model() => "tm-" + Guid.NewGuid().ToString("N");

    private static bool Eventually(Func<bool> cond, int timeoutMs = 5000)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (cond()) return true;
            Thread.Sleep(10);
        }
        return cond();
    }

    // The canonical MemoryPack pairing — the typed overlay carries this end to end with no hand-written codec.
    private static SluiceCodec<Quote> QuoteCodec() => new(
        q => MemoryPackSerializer.Serialize(q),
        span => MemoryPackSerializer.Deserialize<Quote>(span)!);

    [Fact]
    public void Typed_mirror_fetches_and_deserializes_origin_value()
    {
        var model = Model();
        using var host = new ModelHost<Quote>(model, QuoteCodec());
        host.Set("BTC", new Quote(100, 101));
        Thread.Sleep(50);

        using var mirror = new ModelMirror<Quote>(model, QuoteCodec());
        var c = mirror.Get("BTC");

        Assert.True(c.IsConsistent);
        Assert.True(c.Exists);
        Assert.Equal(new Quote(100, 101), c.Value);
    }

    [Fact]
    public void Typed_origin_change_invalidates_then_refetch_sees_new_value()
    {
        var model = Model();
        using var host = new ModelHost<Quote>(model, QuoteCodec());
        host.Set("ETH", new Quote(10, 11));
        Thread.Sleep(50);

        using var mirror = new ModelMirror<Quote>(model, QuoteCodec());
        var first = mirror.Get("ETH");
        Assert.Equal(new Quote(10, 11), first.Value);

        host.Set("ETH", new Quote(20, 21));
        Assert.True(Eventually(() => first.State == ConsistencyState.Invalidated));

        var second = mirror.Get("ETH");
        Assert.True(second.IsConsistent);
        Assert.Equal(new Quote(20, 21), second.Value);
    }

    [Fact]
    public void Typed_mirrored_state_pushes_typed_updates()
    {
        var model = Model();
        using var host = new ModelHost<Quote>(model, QuoteCodec());
        host.Set("SOL", new Quote(1, 2));
        Thread.Sleep(50);

        using var mirror = new ModelMirror<Quote>(model, QuoteCodec());
        var updates = new System.Collections.Concurrent.ConcurrentQueue<Quote>();
        using var state = new MirroredState<Quote>(mirror, "SOL");
        state.Updated += q => updates.Enqueue(q);

        Assert.True(Eventually(() => updates.Contains(new Quote(1, 2))));
        host.Set("SOL", new Quote(3, 4));
        Assert.True(Eventually(() => updates.Contains(new Quote(3, 4))));
    }

    [Fact]
    public void Typed_mirror_reports_missing_key()
    {
        var model = Model();
        using var host = new ModelHost<Quote>(model, QuoteCodec());
        Thread.Sleep(50);

        using var mirror = new ModelMirror<Quote>(model, QuoteCodec());
        var c = mirror.Get("nope");
        Assert.False(c.Exists);
        Assert.Equal(default(Quote), mirror.Value("nope"));
    }
}

[MemoryPackable]
public partial record struct Quote(int Bid, int Ask);
