using Sluice.Fabric;

namespace Sluice.Supergraph;

/// <summary>
/// A globally-addressable vertex in the supergraph: which <see cref="ParticipantId">participant</see> owns it,
/// plus the owner-local key. The owner is the source of truth for the vertex; everyone else mirrors it. Reads of
/// a vertex you own are a direct local read (zero round-trip); reads of a vertex another participant owns route a
/// fetch to that owner over the fabric — local (shared memory, in place) or remote (over the network) depending
/// on where the owner lives.
///
/// <para>
/// <b>The owner address is reach-relative under federation.</b> A vertex owned by a node you reach over the
/// network is addressed by that node's network id (e.g. <c>node:alice</c>); the same vertex, observed by a
/// co-located participant on the owner's host, is addressed by the owner's shared-memory id. Use the owner id as
/// you observe it — it is exactly the <see cref="Inbound.From"/> you receive that vertex's invalidations under,
/// which is also the address its fetch routes back to. (A presence directory that unifies the two into one
/// logical id is a planned upgrade; the routing is correct either way today.)
/// </para>
/// </summary>
public readonly record struct VertexId(ParticipantId Owner, string Key)
{
    public override string ToString() => $"{Owner.Value}/{Key}";
}
