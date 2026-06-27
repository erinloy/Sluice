using System.Diagnostics;
using System.Globalization;

namespace Sluice.Rpc;

/// <summary>
/// Zero-config discovery for the daemon+thin-CLI pattern: a single owner is elected via a system-global named
/// mutex, and publishes a small lease file (pid + heartbeat + capacity) so a freshly-launched CLI process can
/// find the live instance — and tell a crashed one from a running one by heartbeat staleness. Mirrors the
/// named-mutex + lease-file approach proven in the Bitter control plane.
/// </summary>
public static class SluiceDiscovery
{
    private static string LeaseDir => Path.Combine(Path.GetTempPath(), "sluice");
    private static string LeasePath(string endpoint) => Path.Combine(LeaseDir, endpoint + ".lease");

    /// <summary>
    /// Try to become the single owner of <paramref name="endpoint"/>. Returns a disposable ownership handle on
    /// success (dispose to relinquish), or null if another live owner already holds it.
    /// </summary>
    public static IDisposable? TryBecomeOwner(string endpoint)
    {
        var mutex = new Mutex(false, RingNames.OwnerMutex(endpoint));
        bool held;
        try { held = mutex.WaitOne(0); }
        catch (AbandonedMutexException) { held = true; } // previous owner crashed — we inherit it
        if (!held)
        {
            mutex.Dispose();
            return null;
        }
        return new OwnerToken(mutex);
    }

    /// <summary>Write/refresh the lease heartbeat. The owner should call this on start and periodically.</summary>
    public static void Heartbeat(string endpoint, long requestCapacity)
    {
        Directory.CreateDirectory(LeaseDir);
        var line = string.Join(' ',
            Environment.ProcessId.ToString(CultureInfo.InvariantCulture),
            DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture),
            requestCapacity.ToString(CultureInfo.InvariantCulture));
        File.WriteAllText(LeasePath(endpoint), line);
    }

    /// <summary>
    /// Is there a live owner for <paramref name="endpoint"/>? True when a lease exists, its pid is still
    /// running, and its heartbeat is newer than <paramref name="staleAfter"/>.
    /// </summary>
    public static bool IsAlive(string endpoint, TimeSpan staleAfter)
        => TryReadLease(endpoint, out var lease)
           && DateTime.UtcNow - lease.Heartbeat <= staleAfter
           && PidIsRunning(lease.Pid);

    /// <summary>Read the current lease, if any.</summary>
    public static bool TryReadLease(string endpoint, out SluiceLease lease)
    {
        lease = default;
        var path = LeasePath(endpoint);
        if (!File.Exists(path)) return false;
        try
        {
            var parts = File.ReadAllText(path).Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 3) return false;
            lease = new SluiceLease(
                int.Parse(parts[0], CultureInfo.InvariantCulture),
                new DateTime(long.Parse(parts[1], CultureInfo.InvariantCulture), DateTimeKind.Utc),
                long.Parse(parts[2], CultureInfo.InvariantCulture));
            return true;
        }
        catch { return false; } // torn read during a concurrent rewrite — caller treats as "no lease yet"
    }

    private static bool PidIsRunning(int pid)
    {
        try { using var _ = Process.GetProcessById(pid); return true; }
        catch { return false; }
    }

    private sealed class OwnerToken(Mutex mutex) : IDisposable
    {
        public void Dispose()
        {
            try { mutex.ReleaseMutex(); } catch { /* not held / abandoned */ }
            mutex.Dispose();
        }
    }
}

/// <summary>A parsed lease: who owns an endpoint, when they last beat, and the request ring capacity.</summary>
public readonly record struct SluiceLease(int Pid, DateTime Heartbeat, long RequestCapacity);
