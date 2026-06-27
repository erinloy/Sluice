using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using Sluice.Multiway;
using Sluice.Rpc;

namespace Sluice.Fusion;

/// <summary>
/// The mirror side: a process that attaches to a <see cref="ModelHost"/>'s model, caches the keys it reads,
/// and keeps them fresh by listening for the host's invalidations. A read returns the cached snapshot while
/// it is consistent; once the host invalidates a key, the next read refetches the value over RPC and produces
/// a new <see cref="SluiceComputed"/> snapshot — the Fusion invalidate→recompute lifecycle, peer-to-peer over
/// shared memory.
/// </summary>
public sealed class ModelMirror : IDisposable
{
    private readonly ConcurrentDictionary<string, SluiceComputed> _cache = new(StringComparer.Ordinal);
    private readonly ShmTopic _invalidations;
    private readonly ShmMulticast.Subscriber _invalSub;
    private readonly SluiceClient _client;
    private readonly CancellationTokenSource _cts = new();
    private readonly Thread _listener;

    public string Model { get; }

    /// <summary>Raised (on the listener thread) when a cached key is invalidated by the origin.</summary>
    public event Action<string>? Invalidated;

    public ModelMirror(string model)
    {
        Model = model;
        _invalidations = new ShmTopic(ModelHost.InvalTopic(model), maxPayload: 1024, slotCount: 8192, DeliveryMode.Reliable);
        _invalSub = _invalidations.Subscribe();
        _client = new SluiceClient(ModelHost.RpcEndpoint(model));
        _listener = new Thread(ListenLoop) { IsBackground = true, Name = $"fusion-mirror:{model}" };
        _listener.Start();
    }

    /// <summary>
    /// Get the current snapshot for <paramref name="key"/>: the cached value if consistent, otherwise a fresh
    /// fetch from the origin. The returned <see cref="SluiceComputed"/> exposes the value plus invalidation
    /// hooks (<c>WhenInvalidated</c> / <c>Invalidated</c>) for building reactive flows.
    /// </summary>
    public SluiceComputed Get(string key)
    {
        if (_cache.TryGetValue(key, out var c) && c.IsConsistent)
            return c;
        return Fetch(key);
    }

    /// <summary>Convenience: the current value bytes for a key (null if the origin has no such key).</summary>
    public byte[]? Value(string key) => Get(key) is { } c && c.Version >= 0 ? c.Value : null;

    private SluiceComputed Fetch(string key)
    {
        var resp = _client.Send(ModelHost.OpGet, Encoding.UTF8.GetBytes(key));
        SluiceComputed fresh = resp.Ok && resp.Payload.Length >= 8
            ? new SluiceComputed(key, resp.Payload[8..], BinaryPrimitives.ReadInt64LittleEndian(resp.Payload))
            : new SluiceComputed(key, [], -1);
        _cache[key] = fresh;
        return fresh;
    }

    private void ListenLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            if (!_invalSub.Wait(_cts.Token)) break;
            while (_invalSub.TryReadCopy(out var frame))
            {
                var (key, version) = ModelHost.DecodeInval(frame);
                // Only invalidate if we actually cache an older version of this key.
                if (_cache.TryGetValue(key, out var c) && c.Version < version)
                {
                    c.Invalidate();
                    Invalidated?.Invoke(key);
                }
            }
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _listener.Join(TimeSpan.FromSeconds(2));
        _invalSub.Dispose();
        _invalidations.Dispose();
        _client.Dispose();
        _cts.Dispose();
    }
}
