# Sluice documentation

Start at the [project README](../README.md) for the pitch, install, and quick start. These guides go deeper:

- **[Architecture](architecture.md)** — how it actually works: the layers, the zero-copy ring protocol, the
  sequenced multicast ring, the OS floor, and the correctness model.
- **[Choosing a topology](topologies.md)** — the decision table and a copy-paste cookbook for RPC, frame
  channels, topics, peers, gossip, and [Fusion](https://github.com/ActualLab/Fusion).
- **[The fabric](federation.md)** — one channel spanning local shared-memory and remote (TCP) participants
  behind a transport-agnostic seam.
- **[Delivery & safety](delivery-and-safety.md)** — reliable vs lossy, torn-read handling, the memory model, and
  crash robustness, failure mode by failure mode.
- **[Deployment & cross-platform](deployment.md)** — Windows vs Linux internals, container `/dev/shm` sizing, and
  running in Azure Container Apps.
- **[Performance](performance.md)** — the numbers, where they come from, and the knobs that move them.

## Reading order

- *Evaluating Sluice?* README → [topologies](topologies.md) → [performance](performance.md).
- *Adopting it?* [topologies](topologies.md) → [delivery & safety](delivery-and-safety.md) → [deployment](deployment.md).
- *Extending or porting it?* [architecture](architecture.md) → the source under [`src/Sluice`](../src/Sluice).
