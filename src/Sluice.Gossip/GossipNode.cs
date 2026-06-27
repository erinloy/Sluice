using System.Collections.Concurrent;
using System.Text;
using Sluice.Multiway;

namespace Sluice.Gossip;

/// <summary>
/// A node in an epidemic-dissemination cluster over Sluice shared memory. Every node holds a replicated
/// last-writer-wins key/value store; updates spread two ways:
///
/// <list type="number">
/// <item><b>Rumor mongering</b> — a local <see cref="Set"/> immediately broadcasts the entry on a lossy
/// multicast topic, so on one machine it reaches every node in O(1).</item>
/// <item><b>Anti-entropy</b> — each node periodically re-broadcasts a random subset of its entries, which
/// repairs anything a lossy drop missed and brings late joiners up to date. Convergence is eventual.</item>
/// </list>
///
/// Membership is tracked via periodic presence announcements on a second topic; a node is "discovered" once
/// it has heard at least one peer. Everything runs on background threads — the hot path (Set + rumor) is
/// lock-free over a <see cref="ConcurrentDictionary{TKey,TValue}"/> and a single interlocked Lamport clock.
/// </summary>
public sealed class GossipNode : IDisposable
{
    public string ClusterName { get; }
    public string NodeId { get; }

    private readonly ShmTopic _rumors;
    private readonly ShmTopic _members;
    private readonly ConcurrentDictionary<string, GossipEntry> _store = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _peers = new(StringComparer.Ordinal); // nodeId → last-seen ticks
    private readonly int _reinforce;
    private readonly TimeSpan _interval;
    private readonly TimeSpan _peerStaleAfter;
    private readonly CancellationTokenSource _cts = new();
    private readonly List<Thread> _threads = [];

    private long _clock;
    private ShmMulticast.Subscriber _rumorSub = null!;
    private ShmMulticast.Subscriber _memberSub = null!;

    /// <summary>Raised (on a background thread) whenever a key's value is updated locally or via gossip.</summary>
    public event Action<string>? KeyUpdated;

    public GossipNode(string clusterName, string nodeId,
        int reinforceFanout = 8, TimeSpan? gossipInterval = null, int maxNodes = 64)
    {
        ClusterName = clusterName;
        NodeId = nodeId;
        _reinforce = reinforceFanout;
        _interval = gossipInterval ?? TimeSpan.FromMilliseconds(50);
        _peerStaleAfter = TimeSpan.FromMilliseconds(_interval.TotalMilliseconds * 6);
        _rumors = new ShmTopic($"gossip.{clusterName}.rumors", maxPayload: 8192, slotCount: 8192, DeliveryMode.Lossy, maxNodes);
        _members = new ShmTopic($"gossip.{clusterName}.members", maxPayload: 256, slotCount: 1024, DeliveryMode.Lossy, maxNodes);
    }

    /// <summary>Number of live peers heard from (excludes self).</summary>
    public int LivePeerCount => _peers.Count;

    /// <summary>True once at least one peer has been discovered.</summary>
    public bool IsDiscovered => _peers.Count > 0;

    public IReadOnlyCollection<string> Keys => (IReadOnlyCollection<string>)_store.Keys;

    public void Start()
    {
        _rumorSub = _rumors.Subscribe();
        _memberSub = _members.Subscribe();
        StartThread(RumorLoop);
        StartThread(MemberLoop);
        StartThread(AntiEntropyLoop);
        StartThread(AnnounceLoop);
        Announce(); // announce presence immediately so peers discover us fast
    }

    /// <summary>Set a key locally and immediately gossip it to the cluster.</summary>
    public void Set(string key, ReadOnlySpan<byte> value)
    {
        long v = Interlocked.Increment(ref _clock);
        var entry = new GossipEntry(NodeId, v, key, value.ToArray());
        _store[key] = entry;
        Broadcast(entry);
        KeyUpdated?.Invoke(key);
    }

    public bool TryGet(string key, out byte[] value)
    {
        if (_store.TryGetValue(key, out var e)) { value = e.Value; return true; }
        value = [];
        return false;
    }

    // ---- internals ---------------------------------------------------------------------------------

    private void Apply(GossipEntry incoming)
    {
        ObserveClock(incoming.Version);
        bool changed = false;
        _store.AddOrUpdate(incoming.Key, _ => { changed = true; return incoming; },
            (_, existing) =>
            {
                if (GossipEntry.Supersedes(incoming, existing)) { changed = true; return incoming; }
                return existing;
            });
        if (changed) KeyUpdated?.Invoke(incoming.Key);
    }

    private void ObserveClock(long seen)
    {
        long cur;
        while ((cur = Interlocked.Read(ref _clock)) < seen
               && Interlocked.CompareExchange(ref _clock, seen, cur) != cur) { }
    }

    private void Broadcast(in GossipEntry entry) => _rumors.Publish(entry.Encode());

    private void RumorLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            if (!_rumorSub.Wait(_cts.Token)) break;
            while (_rumorSub.TryReadCopy(out var frame))
                Apply(GossipEntry.Decode(frame));
        }
    }

    private void MemberLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            if (!_memberSub.Wait(_cts.Token)) break;
            while (_memberSub.TryReadCopy(out var frame))
            {
                var id = Encoding.UTF8.GetString(frame);
                if (id != NodeId) _peers[id] = DateTime.UtcNow.Ticks;
            }
        }
    }

    private void AntiEntropyLoop()
    {
        while (!Sleep()) // periodic reinforcement: re-gossip a random subset to repair drops + late joiners
        {
            var snapshot = _store.Values.ToArray();
            if (snapshot.Length == 0) continue;
            for (int i = 0; i < Math.Min(_reinforce, snapshot.Length); i++)
                Broadcast(snapshot[Random.Shared.Next(snapshot.Length)]);
        }
    }

    private void AnnounceLoop()
    {
        while (!Sleep())
        {
            Announce();
            long cutoff = DateTime.UtcNow.Ticks - _peerStaleAfter.Ticks;
            foreach (var kv in _peers)
                if (kv.Value < cutoff) _peers.TryRemove(kv.Key, out _);
        }
    }

    private void Announce() => _members.Publish(Encoding.UTF8.GetBytes(NodeId));

    private bool Sleep()
    {
        try { return _cts.Token.WaitHandle.WaitOne(_interval); } // returns true if cancelled
        catch { return true; }
    }

    private void StartThread(Action loop)
    {
        var t = new Thread(() => loop()) { IsBackground = true, Name = $"gossip:{NodeId}" };
        _threads.Add(t);
        t.Start();
    }

    public void Dispose()
    {
        _cts.Cancel();
        foreach (var t in _threads) t.Join(TimeSpan.FromSeconds(2));
        _rumorSub?.Dispose();
        _memberSub?.Dispose();
        _rumors.Dispose();
        _members.Dispose();
        _cts.Dispose();
    }
}
