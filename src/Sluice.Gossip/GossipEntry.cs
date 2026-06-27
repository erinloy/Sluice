using System.Buffers.Binary;
using System.Text;

namespace Sluice.Gossip;

/// <summary>
/// One replicated key/value record, tagged with a Lamport <see cref="Version"/> and its <see cref="Origin"/>
/// node. Conflicts resolve last-writer-wins by (version, origin) so every node converges deterministically.
/// </summary>
public readonly struct GossipEntry(string origin, long version, string key, byte[] value)
{
    public string Origin { get; } = origin;
    public long Version { get; } = version;
    public string Key { get; } = key;
    public byte[] Value { get; } = value;

    /// <summary>True if <paramref name="a"/> supersedes <paramref name="b"/> under the LWW rule.</summary>
    public static bool Supersedes(in GossipEntry a, in GossipEntry b)
        => a.Version > b.Version || (a.Version == b.Version && string.CompareOrdinal(a.Origin, b.Origin) > 0);

    // Wire layout: [long version][int originLen][origin][int keyLen][key][int valueLen][value].
    public byte[] Encode()
    {
        var origin = Encoding.UTF8.GetBytes(Origin);
        var key = Encoding.UTF8.GetBytes(Key);
        var buf = new byte[8 + 4 + origin.Length + 4 + key.Length + 4 + Value.Length];
        var span = buf.AsSpan();
        BinaryPrimitives.WriteInt64LittleEndian(span, Version);
        int o = 8;
        o = WriteBlock(span, o, origin);
        o = WriteBlock(span, o, key);
        WriteBlock(span, o, Value);
        return buf;
    }

    public static GossipEntry Decode(ReadOnlySpan<byte> span)
    {
        long version = BinaryPrimitives.ReadInt64LittleEndian(span);
        int o = 8;
        var origin = ReadBlock(span, ref o);
        var key = ReadBlock(span, ref o);
        var value = ReadBlock(span, ref o);
        return new GossipEntry(Encoding.UTF8.GetString(origin), version, Encoding.UTF8.GetString(key), value.ToArray());
    }

    private static int WriteBlock(Span<byte> span, int offset, ReadOnlySpan<byte> block)
    {
        BinaryPrimitives.WriteInt32LittleEndian(span[offset..], block.Length);
        block.CopyTo(span[(offset + 4)..]);
        return offset + 4 + block.Length;
    }

    private static ReadOnlySpan<byte> ReadBlock(ReadOnlySpan<byte> span, ref int offset)
    {
        int len = BinaryPrimitives.ReadInt32LittleEndian(span[offset..]);
        var block = span.Slice(offset + 4, len);
        offset += 4 + len;
        return block;
    }
}
