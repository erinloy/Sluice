namespace Sluice.Multiway;

/// <summary>
/// A named publish/subscribe topic over a <see cref="ShmMulticast"/> ring. Any process can construct the same
/// topic by name; the first one creates it. One publisher gives broadcast (1→N); many publishers give a full
/// many-to-many bus (N↔N). Delivery (reliable vs lossy) is chosen by the creator.
/// </summary>
public sealed class ShmTopic : IDisposable
{
    private readonly ShmMulticast _ring;

    /// <param name="name">Logical topic name (shared by all participants).</param>
    /// <param name="maxPayload">Fixed per-message capacity.</param>
    /// <param name="slotCount">Ring depth (rounded up to a power of two).</param>
    /// <param name="mode">Reliable (never drop, backpressure) or Lossy (never block, overwrite-oldest).</param>
    /// <param name="maxConsumers">Maximum concurrent subscribers.</param>
    public ShmTopic(string name, int maxPayload = 256, int slotCount = 1024,
        DeliveryMode mode = DeliveryMode.Lossy, int maxConsumers = 64)
        => _ring = ShmMulticast.CreateOrAttach("sluice.topic." + name, maxPayload, slotCount, mode, maxConsumers);

    public DeliveryMode Mode => _ring.Mode;
    public int MaxPayload => _ring.MaxPayload;

    /// <summary>Publish a message to every current subscriber. Returns its global sequence number.</summary>
    public long Publish(ReadOnlySpan<byte> payload, CancellationToken ct = default) => _ring.Publish(payload, ct);

    /// <summary>Open an independent subscription that will see every message published from now on.</summary>
    public ShmMulticast.Subscriber Subscribe() => _ring.Subscribe();

    public void Dispose() => _ring.Dispose();
}
