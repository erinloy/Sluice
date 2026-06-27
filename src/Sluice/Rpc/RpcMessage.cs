using System.Runtime.InteropServices;

namespace Sluice.Rpc;

/// <summary>
/// The fixed-size, blittable RPC envelope that prefixes every frame on the wire. Because it is
/// <c>unmanaged</c> with explicit sequential layout, the receiver reads it <b>in place</b> over the shared
/// memory via <c>MemoryMarshal.AsRef&lt;RpcHeader&gt;(span)</c> — a pointer reinterpret, no deserialize.
/// The payload follows immediately after the header in the same frame.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 32)]
public readonly struct RpcHeader
{
    public readonly Guid CorrelationId;   // 16 — pairs a response (or stream) with its request
    public readonly long ClientId;        // 8  — routes the response to the caller's response ring
    public readonly int Kind;             // 4  — application opcode
    public readonly RpcFlags Flags;       // 4  — request/response/stream framing bits

    public RpcHeader(Guid correlationId, long clientId, int kind, RpcFlags flags)
    {
        CorrelationId = correlationId;
        ClientId = clientId;
        Kind = kind;
        Flags = flags;
    }

    public static int Size => 32;
}

[Flags]
public enum RpcFlags
{
    None = 0,
    Response = 1 << 0,   // owner → client (vs a client → owner request)
    Ok = 1 << 1,         // the response succeeded
    StreamItem = 1 << 2, // one element of a streamed response
    StreamEnd = 1 << 3,  // terminal marker of a stream (carries no payload)
}

/// <summary>A completed unary response handed back to the caller.</summary>
public sealed class RpcResponse
{
    public bool Ok { get; }
    public byte[] Payload { get; }

    public RpcResponse(bool ok, byte[] payload)
    {
        Ok = ok;
        Payload = payload;
    }

    public string Text => System.Text.Encoding.UTF8.GetString(Payload);
}
