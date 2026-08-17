using System.Buffers.Binary;
using Sluice.Fabric;
using Sluice.Fusion;
using Sluice.Supergraph;

// Sluice.Supergraph demo — a federated reactive graph.
//
// Three participants form one logical graph over shared memory:
//   prices  : owns  spot          (a source vertex)
//   risk    : owns  exposure = spot * size   (a computed vertex depending on prices.spot)
//   desk    : observes risk.exposure         (a reactive view, refetched on every change)
//
// Updating spot on `prices` ripples prices -> risk -> desk with no direct prices<->desk link: the supergraph
// reacts as one graph. This is Fusion's invalidate-then-refetch model, federated over Sluice.Fabric — the same
// model whether a participant is co-located (shared memory, read in place) or across the network (over TCP).

var codec = new SluiceCodec<double>(
    d => { var b = new byte[8]; BinaryPrimitives.WriteDoubleLittleEndian(b, d); return b; },
    span => BinaryPrimitives.ReadDoubleLittleEndian(span));

const string graph = "demo.book";
const double size = 100.0;

using var pricesT = new ShmTransport("prices");
using var riskT = new ShmTransport("risk");
using var deskT = new ShmTransport("desk");
using var prices = new GraphPeer(pricesT, graph);
using var risk = new GraphPeer(riskT, graph);
using var desk = new GraphPeer(deskT, graph);

// prices owns the spot vertex.
var spot = prices.Define("spot", codec);
spot.Set(42.00);

// risk computes exposure = spot * size, depending on a vertex owned by another participant.
using var exposure = risk.Computed("exposure", codec,
    () => risk.Get(spot.Id, codec).Value * size,
    spot.Id);

// desk observes exposure and prints every time it changes.
using var view = desk.Observe(exposure.Id, codec);
view.Updated += v =>
    Console.WriteLine($"  desk sees exposure = {v,12:N2}   (read {view.Reach}, v{view.Version})");

Console.WriteLine("federated reactive graph:  prices.spot -> risk.exposure -> desk.view");
Console.WriteLine($"  participants present on the graph: {desk.Participants.Count + 1}\n");

foreach (var px in new[] { 42.00, 43.25, 41.10, 50.00 })
{
    Console.WriteLine($"prices.spot <- {px:N2}");
    spot.Set(px);
    Thread.Sleep(250);   // let the change ripple prices -> risk -> desk
}

Console.WriteLine("\ndone — every spot change propagated across two participant boundaries reactively.");
