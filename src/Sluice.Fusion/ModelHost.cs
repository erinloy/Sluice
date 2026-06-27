using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using Sluice.Multiway;
using Sluice.Rpc;

namespace Sluice.Fusion;

/// <summary>
/// The origin side of a Fusion-style model: it owns the source-of-truth values and, on every change,
/// publishes a tiny <b>invalidation</b> (key + new version) on a Sluice topic — it never pushes the value.
/// Mirrors refetch the value lazily, on next access, via the host's RPC endpoint. This is exactly Fusion's
/// "signal stale, recompute on demand" model: minimal traffic, eventual freshness.
/// </summary>
public sealed class ModelHost : IDisposable
{
    internal const int OpGet = 1;

    private readonly ConcurrentDictionary<string, (long Version, byte[] Value)> _store = new(StringComparer.Ordinal);
    private readonly ShmTopic _invalidations;
    private readonly SluiceServer _server;
    private long _version;

    public string Model { get; }

    public ModelHost(string model)
    {
        Model = model;
        // Reliable: an invalidation must never be dropped, or a mirror would serve stale data forever.
        _invalidations = new ShmTopic(InvalTopic(model), maxPayload: 1024, slotCount: 8192, DeliveryMode.Reliable);
        _server = new SluiceServer(RpcEndpoint(model), Handle);
        _server.Start();
        SluiceDiscovery.Heartbeat(RpcEndpoint(model), 1 << 20);
    }

    /// <summary>Set a key and invalidate every mirror's cached copy of it.</summary>
    public void Set(string key, byte[] value)
    {
        long v = Interlocked.Increment(ref _version);
        _store[key] = (v, value);
        _invalidations.Publish(EncodeInval(key, v));
    }

    public bool TryGet(string key, out byte[] value)
    {
        if (_store.TryGetValue(key, out var e)) { value = e.Value; return true; }
        value = [];
        return false;
    }

    private void Handle(in RpcContext ctx)
    {
        if (ctx.Kind != OpGet) { ctx.Reply(ReadOnlySpan<byte>.Empty, ok: false); return; }
        var key = Encoding.UTF8.GetString(ctx.Request);
        if (_store.TryGetValue(key, out var e))
        {
            // reply = [version(8)][value]
            var buf = new byte[8 + e.Value.Length];
            BinaryPrimitives.WriteInt64LittleEndian(buf, e.Version);
            e.Value.CopyTo(buf.AsSpan(8));
            ctx.Reply(buf);
        }
        else ctx.Reply(ReadOnlySpan<byte>.Empty, ok: false);
    }

    internal static string InvalTopic(string model) => $"fusion.{model}.inval";
    internal static string RpcEndpoint(string model) => $"fusion.{model}.rpc";

    // invalidation wire: [version(8)][key utf8]
    internal static byte[] EncodeInval(string key, long version)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var buf = new byte[8 + keyBytes.Length];
        BinaryPrimitives.WriteInt64LittleEndian(buf, version);
        keyBytes.CopyTo(buf.AsSpan(8));
        return buf;
    }

    internal static (string Key, long Version) DecodeInval(ReadOnlySpan<byte> frame)
        => (Encoding.UTF8.GetString(frame[8..]), BinaryPrimitives.ReadInt64LittleEndian(frame));

    public void Dispose()
    {
        _server.Dispose();
        _invalidations.Dispose();
    }
}
