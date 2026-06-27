using Sluice.Rpc;
using Xunit;

namespace Sluice.Tests;

public class DiscoveryTests
{
    private static string Endpoint() => "disc-" + Guid.NewGuid().ToString("N");

    // The contender runs on its own thread because a named Mutex is re-entrant within a single thread —
    // a second thread faithfully models the "second process" the discovery contract actually guards against.
    private static IDisposable? ContendFromAnotherThread(string endpoint)
    {
        IDisposable? result = null;
        var t = new Thread(() => result = SluiceDiscovery.TryBecomeOwner(endpoint));
        t.Start();
        t.Join();
        return result;
    }

    [Fact]
    public void First_owner_wins_second_is_refused_until_released()
    {
        var endpoint = Endpoint();

        var first = SluiceDiscovery.TryBecomeOwner(endpoint);
        Assert.NotNull(first);

        Assert.Null(ContendFromAnotherThread(endpoint));   // a second process is refused

        first!.Dispose();                                  // relinquish

        var third = ContendFromAnotherThread(endpoint);
        Assert.NotNull(third);                             // now claimable again
        third!.Dispose();
    }

    [Fact]
    public void Heartbeat_makes_endpoint_alive_and_lease_is_readable()
    {
        var endpoint = Endpoint();
        Assert.False(SluiceDiscovery.IsAlive(endpoint, TimeSpan.FromSeconds(5)));

        SluiceDiscovery.Heartbeat(endpoint, requestCapacity: 1 << 20);

        Assert.True(SluiceDiscovery.IsAlive(endpoint, TimeSpan.FromSeconds(5)));
        Assert.True(SluiceDiscovery.TryReadLease(endpoint, out var lease));
        Assert.Equal(Environment.ProcessId, lease.Pid);
        Assert.Equal(1 << 20, lease.RequestCapacity);
    }

    [Fact]
    public void Stale_heartbeat_is_not_alive()
    {
        var endpoint = Endpoint();
        SluiceDiscovery.Heartbeat(endpoint, 1 << 20);
        // A zero-tolerance staleness window makes even a fresh beat read as stale.
        Assert.False(SluiceDiscovery.IsAlive(endpoint, TimeSpan.Zero));
    }
}
