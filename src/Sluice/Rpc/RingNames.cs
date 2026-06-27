namespace Sluice.Rpc;

/// <summary>Central place for the system-global names that bind a client to an owner for an endpoint.</summary>
internal static class RingNames
{
    public static string Request(string endpoint) => $"sluice.{endpoint}.req";
    public static string Response(string endpoint, long clientId) => $"sluice.{endpoint}.resp.{clientId:x}";
    public static string RequestMutex(string endpoint) => $"sluice.{endpoint}.req.mtx";
    public static string OwnerMutex(string endpoint) => $"sluice.{endpoint}.owner";
}
