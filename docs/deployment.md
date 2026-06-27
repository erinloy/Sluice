# Deployment & cross-platform

Sluice runs on Windows and Linux from one codebase. This guide covers the platform differences that matter in
production and how to run it in containers — including Azure Container Apps, which is a primary target.

## What differs between Windows and Linux

Everything is centralized in [`ShmMap`](architecture.md#layer-0--shmmap-the-os-floor); the rest of the library is
platform-agnostic.

| Concern        | Windows                              | Linux / Unix                                            |
|----------------|--------------------------------------|---------------------------------------------------------|
| Shared region  | named, page-file-backed MMF          | file-backed MMF under `/dev/shm` (tmpfs → RAM-resident)  |
| Doorbell       | named `EventWaitHandle` (sub-µs wake) | none → callers poll (escalating backoff, ≤ 1 ms)        |
| Discovery lock | named `Mutex`                        | named `Mutex` (works on Unix)                           |
| Lease file     | temp dir                             | temp dir                                                |
| Cleanup        | region frees with last handle        | owner unlinks the tmpfs backing file on dispose         |

**The only behavioral difference is idle wakeup latency.** Under active load, messages land during the spin
phase on both platforms, so throughput and the in-place read path are identical. A *blocked, idle* reader wakes
in sub-microseconds on Windows (wait handle) and within ~1 ms on Linux (the polling cap). For request/response
and streaming workloads this is invisible; it only matters if you have a reader that is idle for long stretches
and needs the very first message after idle to arrive with minimal latency.

## The shared-memory boundary

Sluice is a **same-host** transport. Two processes can share a Sluice region only if they share the same memory
namespace:

- **Multiple processes in one container** (the daemon + thin-CLI pattern, or a worker pool): always works — they
  share the container's `/dev/shm`.
- **Multiple containers**: only if they share an IPC/volume — e.g. co-located containers mounting the **same
  volume at the same path** for the backing files, or sharing an IPC namespace. Containers that don't share
  memory cannot share a Sluice region; use a network transport between them and Sluice within each.

This is the same constraint as any shared-memory IPC — it is not a network protocol and has no cross-host story.

## Containers: sizing `/dev/shm`

On Linux a ring's backing file lives in `/dev/shm` (tmpfs), which counts against the container's shared-memory
limit. **The default is often 64 MB**, which is plenty for typical RPC/topic use but easy to exceed if you
allocate many or large rings.

Budget it: each `ShmRing` is about `256 B + capacity` (default capacity 1 MiB), and each multicast ring is about
`headers + slotCount × slotSize`. Sum your rings and keep under the limit, or raise it.

**Docker:**

```bash
docker run --shm-size=256m myimage
```

**Kubernetes** (an in-memory `emptyDir` mounted at `/dev/shm`):

```yaml
spec:
  containers:
    - name: app
      volumeMounts:
        - name: dshm
          mountPath: /dev/shm
  volumes:
    - name: dshm
      emptyDir:
        medium: Memory
        sizeLimit: 256Mi
```

If `/dev/shm` is missing entirely, Sluice falls back to the system temp dir — which is **disk-backed**, so the
zero-copy reads still work but writes hit real I/O. Prefer a tmpfs mount.

## Azure Container Apps

ACA runs Linux containers, so Sluice uses the `/dev/shm` path automatically. Two things to set up:

1. **Co-locate the processes that share a region.** Sluice is same-host IPC. Put the daemon and its clients in
   the **same container** (multiple processes), or use ACA's multi-container apps with a **shared memory volume**
   mounted at the same path in each container that participates. Separate ACA *apps* (separate hosts) cannot
   share a Sluice region.
2. **Size `/dev/shm`.** Mount an in-memory `EmptyDir`-style volume at `/dev/shm` sized for your rings, the same
   as the Kubernetes example above. ACA volume mounts support `storageType: EmptyDir` for ephemeral memory.

Targeting `net8.0` is fine — the libraries multi-target `net8.0;net10.0`, and `net8.0` is the ACA LTS runtime.

## Verifying on Linux locally

You don't need a Linux box to check the Linux path — run the suite in a container:

```bash
docker run --rm --shm-size=256m -v "$PWD:/src" -w /src \
  mcr.microsoft.com/dotnet/sdk:10.0 \
  dotnet test Sluice.slnx -c Release
```

This exercises the file-backed maps, the `FileMode.CreateNew` multicast race, the polling doorbell, named-`Mutex`
discovery, and the temp-dir lease end-to-end on a real Linux kernel — the same suite that is green on Windows.

## Operational notes

- **Stale backing files (Linux).** The owner unlinks its tmpfs file on clean dispose. A hard kill (SIGKILL) can
  leave a file in `/dev/shm`; it is reclaimed on container restart (tmpfs is volatile) and a new owner overwrites
  it. For long-lived hosts, a janitor that removes `sluice-*.shm` older than your max process lifetime is a
  reasonable belt-and-suspenders.
- **Crashed owners.** Discovery distinguishes a crashed owner from a live one via the lease (pid liveness +
  heartbeat freshness) and surfaces an `AbandonedMutexException` as inherited ownership — so a new daemon can
  reclaim an endpoint cleanly. See [delivery-and-safety](delivery-and-safety.md).
- **Permissions.** All participants must have read/write to the same `/dev/shm` (or temp dir) path and run as
  users that can open the named `Mutex`. Within one container this is automatic.
