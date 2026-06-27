# Security Policy

## Reporting a vulnerability

Please report security issues privately rather than opening a public issue. Use GitHub's
[private vulnerability reporting](https://github.com/erinloy/Sluice/security/advisories/new) for this repository,
and you'll get an acknowledgement and a fix or mitigation plan.

Please include a minimal reproduction and the affected version/commit.

## Threat model — read this first

Sluice is **same-host IPC for a cooperative process family**. Understanding its trust model prevents most
misuse:

- **A Sluice region is not a security boundary.** Any process that can open the named shared region (Windows) or
  the backing file under `/dev/shm` / the temp dir (Linux) can read and write every message on it. Scope access
  with OS permissions on those objects — run participants as the same user, restrict the `/dev/shm` path, and do
  not share a region across a trust boundary.
- **Payloads are not validated.** The bytes in a slot are whatever the producer wrote. Reading a blittable struct
  *in place* from a producer means trusting that producer's layout — which is exactly the intended design within
  a cooperative family, and exactly what you must not do across a trust boundary. If a producer may be hostile,
  validate before interpreting, and don't reinterpret untrusted bytes as a struct.
- **No durability, no confidentiality at rest.** Data lives in RAM (or tmpfs); there is no encryption. tmpfs
  backing files are world-visible subject to filesystem permissions.
- **Not a network transport.** There is no remote attack surface from Sluice itself — it never opens a socket.
  The exposure is local: other processes on the same host with access to the named objects / backing files.

## What counts as a vulnerability here

Because of the threat model above, the following are **in scope** (genuine bugs we want to fix):

- A memory-safety defect: a path that hands out a span outside the mapped region, a framing bug that lets a
  reader read past a payload, an integer-overflow in cursor/offset math, or a torn read that isn't detected on a
  path that claims safety.
- A way for a *cooperative but buggy* participant to corrupt or hang others beyond the documented backpressure
  semantics (e.g. crash the daemon despite the per-request isolation, or a wrap/lap-detection error that yields
  wrong data rather than a counted drop).
- Default object names/permissions that are broader than necessary.

**Out of scope** (working as designed, per the threat model): one local process with access to a region reading
or overwriting another's messages; reinterpreting untrusted payloads you chose to trust; lack of encryption at
rest; loss of data on crash/restart.

## Supported versions

During pre-1.0, fixes land on the latest release. Once 1.0 ships, the supported window will be documented here.
