using System.Runtime.InteropServices;
using MemoryPack;
using MessagePack;

namespace Sluice.Benchmarks;

// One logical message, expressed for each codec under test. Field set is deliberately identical so the
// comparison is apples-to-apples: a correlation id, an opcode, a sequence, and a variable byte payload.

[MemoryPackable]
public partial class MemoryPackMsg
{
    public Guid Id { get; set; }
    public int Kind { get; set; }
    public long Seq { get; set; }
    public byte[] Payload { get; set; } = [];
}

[MessagePackObject]
public class MessagePackMsg
{
    [Key(0)] public Guid Id { get; set; }
    [Key(1)] public int Kind { get; set; }
    [Key(2)] public long Seq { get; set; }
    [Key(3)] public byte[] Payload { get; set; } = [];
}

public class JsonMsg
{
    public Guid Id { get; set; }
    public int Kind { get; set; }
    public long Seq { get; set; }
    public byte[] Payload { get; set; } = [];
}

/// <summary>
/// The blittable header for the zero-serialization path: written with <c>MemoryMarshal.Write</c> and read
/// back in place with <c>MemoryMarshal.AsRef</c> — no allocation, no field-by-field decode. The payload
/// rides immediately after it and is handed out as a span over the same buffer.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 32)]
public readonly struct BlittableHeader
{
    public readonly Guid Id;
    public readonly int Kind;
    public readonly long Seq;
    public readonly int PayloadLength;

    public BlittableHeader(Guid id, int kind, long seq, int payloadLength)
    {
        Id = id;
        Kind = kind;
        Seq = seq;
        PayloadLength = payloadLength;
    }
}
