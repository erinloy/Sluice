using System.Text;
using Sluice.Fusion;
using Xunit;

namespace Sluice.Tests;

public class FusionOverlayTests
{
    private static string Model() => "m-" + Guid.NewGuid().ToString("N");

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

    [Fact]
    public void Mirror_fetches_and_caches_origin_value()
    {
        var model = Model();
        using var host = new ModelHost(model);
        host.Set("greeting", "hello"u8.ToArray());
        Thread.Sleep(50);

        using var mirror = new ModelMirror(model);
        var c1 = mirror.Get("greeting");
        Assert.True(c1.IsConsistent);
        Assert.Equal("hello", Encoding.UTF8.GetString(c1.Value));

        // Second read is served from cache — same snapshot instance, still consistent.
        var c2 = mirror.Get("greeting");
        Assert.Same(c1, c2);
    }

    [Fact]
    public void Origin_change_invalidates_the_mirror_then_refetch_sees_new_value()
    {
        var model = Model();
        using var host = new ModelHost(model);
        host.Set("price", "10"u8.ToArray());
        Thread.Sleep(50);

        using var mirror = new ModelMirror(model);
        var first = mirror.Get("price");
        Assert.Equal("10", Encoding.UTF8.GetString(first.Value));
        Assert.True(first.IsConsistent);

        // The origin changes the value -> a tiny invalidation propagates -> the cached snapshot goes stale.
        host.Set("price", "20"u8.ToArray());
        Assert.True(Eventually(() => first.State == ConsistencyState.Invalidated));
        Assert.True(first.WhenInvalidated.IsCompleted);

        // Next read refetches lazily and produces a fresh, consistent snapshot.
        var second = mirror.Get("price");
        Assert.NotSame(first, second);
        Assert.True(second.IsConsistent);
        Assert.Equal("20", Encoding.UTF8.GetString(second.Value));
    }

    [Fact]
    public void MirroredState_pushes_every_update()
    {
        var model = Model();
        using var host = new ModelHost(model);
        host.Set("counter", "0"u8.ToArray());
        Thread.Sleep(50);

        using var mirror = new ModelMirror(model);
        var updates = new System.Collections.Concurrent.ConcurrentQueue<string>();
        using var state = new MirroredState(mirror, "counter");
        state.Updated += v => updates.Enqueue(Encoding.UTF8.GetString(v));

        Assert.True(Eventually(() => updates.Contains("0")));
        host.Set("counter", "1"u8.ToArray());
        Assert.True(Eventually(() => updates.Contains("1")));
        host.Set("counter", "2"u8.ToArray());
        Assert.True(Eventually(() => updates.Contains("2")));
    }
}
