using System.IO.MemoryMappedFiles;

namespace Sluice;

/// <summary>
/// The one place that knows how a Sluice shared region is backed on each OS. On Windows that is a named
/// (page-file-backed) memory-mapped file; on Linux/Unix — where named maps are unsupported in .NET — it is a
/// file-backed map under <c>/dev/shm</c> (tmpfs, so still RAM-resident, not real disk I/O). Every ring and
/// multicast goes through here so the platform split lives in exactly one body.
/// </summary>
internal static class ShmMap
{
    private const MemoryMappedFileAccess RW = MemoryMappedFileAccess.ReadWrite;

    /// <summary>True when regions are backed by real files (Unix) rather than named OS objects (Windows).</summary>
    internal static bool IsFileBacked => !OperatingSystem.IsWindows();

    /// <summary>tmpfs (<c>/dev/shm</c>) when present so the "file" is RAM-resident, else the temp dir.</summary>
    private static string BackingDir() => Directory.Exists("/dev/shm") ? "/dev/shm" : Path.GetTempPath();

    /// <summary>Map a logical region name to a deterministic backing-file path (Unix only).</summary>
    internal static string BackingPath(string name)
    {
        var invalid = Path.GetInvalidFileNameChars();
        Span<char> safe = stackalloc char[name.Length];
        for (int i = 0; i < name.Length; i++)
            safe[i] = Array.IndexOf(invalid, name[i]) >= 0 ? '_' : name[i];
        return Path.Combine(BackingDir(), "sluice-" + new string(safe) + ".shm");
    }

    /// <summary>Open the region if it exists, else create it. Non-atomic on creation — for single-owner rings.</summary>
    internal static MemoryMappedFile OpenOrCreate(string name, long size, out string? backingFile)
    {
        if (OperatingSystem.IsWindows())
        {
            backingFile = null;
            return MemoryMappedFile.CreateOrOpen(name, size, RW);
        }
        backingFile = BackingPath(name);
        return MemoryMappedFile.CreateFromFile(backingFile, FileMode.OpenOrCreate, null, size, RW);
    }

    /// <summary>
    /// Create the region, throwing <see cref="IOException"/> if it already exists. This is the creation-race
    /// arbiter for <c>CreateOrAttach</c>: on Windows it is <see cref="MemoryMappedFile.CreateNew(string,long)"/>;
    /// on Unix the filesystem provides the same atomicity via <see cref="FileMode.CreateNew"/>.
    /// </summary>
    internal static MemoryMappedFile CreateNew(string name, long size, out string? backingFile)
    {
        if (OperatingSystem.IsWindows())
        {
            backingFile = null;
            return MemoryMappedFile.CreateNew(name, size, RW);
        }
        backingFile = BackingPath(name);
        // Atomic create-or-fail, then size before mapping (an opener that races in retries until it's sized).
        using (var fs = new FileStream(backingFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite))
            fs.SetLength(size);
        return MemoryMappedFile.CreateFromFile(backingFile, FileMode.Open, null, size, RW);
    }

    /// <summary>Open an existing region; throws if the owner isn't present.</summary>
    internal static MemoryMappedFile OpenExisting(string name)
    {
        if (OperatingSystem.IsWindows())
            return MemoryMappedFile.OpenExisting(name, MemoryMappedFileRights.ReadWrite);

        var path = BackingPath(name);
        if (!File.Exists(path))
            throw new FileNotFoundException($"Sluice region '{name}' backing file not found (owner not running?).", path);
        // capacity 0 => use the file's current length (the owner sized it).
        return MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, RW);
    }

    /// <summary>An auto-reset doorbell wait handle — named on Windows, null on Unix (callers poll instead).</summary>
    internal static EventWaitHandle? CreateDoorbell(string name)
        => OperatingSystem.IsWindows() ? new EventWaitHandle(false, EventResetMode.AutoReset, name + ".bell") : null;

    /// <summary>Open an existing doorbell — named on Windows, null on Unix.</summary>
    internal static EventWaitHandle? OpenDoorbell(string name)
        => OperatingSystem.IsWindows() ? EventWaitHandle.OpenExisting(name + ".bell") : null;

    /// <summary>Best-effort unlink of a Unix backing file (no-op when null / on Windows).</summary>
    internal static void Unlink(string? backingFile)
    {
        if (backingFile is null) return;
        try { File.Delete(backingFile); } catch { /* best-effort cleanup */ }
    }
}
