# Contributing to Sluice

Thanks for your interest. Sluice is a small, focused library with a high correctness bar — it hands out spans
that point into shared memory across process boundaries, so "looks right" isn't enough. This guide is how to
build it, what to test, and the conventions that keep it coherent.

## Build & test

Requires the .NET SDK (8.0+ to build the libraries; the test and benchmark projects target net10).

```bash
dotnet build  Sluice.slnx -c Release
dotnet test   Sluice.slnx -c Release
```

The whole suite should be green before you push. To exercise the **Linux** path from a non-Linux box:

```bash
docker run --rm --shm-size=256m -v "$PWD:/src" -w /src \
  mcr.microsoft.com/dotnet/sdk:10.0 dotnet test Sluice.slnx -c Release
```

Benchmarks:

```bash
dotnet run -c Release --project bench/Sluice.Benchmarks -- --filter "*RoundTrip*"
dotnet run -c Release --project bench/Sluice.Benchmarks -- throughput
dotnet run -c Release --project bench/Sluice.Benchmarks -- lifecycle
```

Systematic concurrency tests (Coyote) — required if you touch the ring/cursor protocol under `src/Sluice`:

```bash
dotnet tool install --global Microsoft.Coyote.CLI --version 1.7.11   # one-time
./scripts/run-coyote.sh        # build → rewrite → explore; pwsh: ./scripts/run-coyote.ps1
```

This is a separate project (`tests/Sluice.Concurrency`, net8.0 — the Coyote CLI host) and is **not** in
`Sluice.slnx`, so the normal build/test never touches it. CI runs it as the `concurrency-check` job. See
[docs/delivery-and-safety.md](docs/delivery-and-safety.md#systematic-concurrency-testing) for what it proves.

## Project layout

```
src/Sluice              core: ShmRing, ShmMulticast, ShmMap, RPC, Multiway, frame channel
src/Sluice.Gossip       epidemic LWW store
src/Sluice.Fusion       attach/mirror/cache overlay (+ typed generics)
tests/Sluice.Tests      xUnit suite
tests/Sluice.Concurrency  Coyote systematic-concurrency tests (run via scripts/run-coyote.*)
bench/…                 BenchmarkDotNet comparisons + lifecycle/throughput harnesses
samples/Sluice.Demo     the kvd daemon+CLI demo
docs/                   the deep-dive guides
```

Read [docs/architecture.md](docs/architecture.md) before changing anything under `src/Sluice` — the layering
(OS floor → rings → RPC/frame/multiway → extensions) is deliberate and acyclic.

## Conventions

- **Match the surrounding code.** Comment density, naming, and idiom should be indistinguishable from the file
  you're editing.
- **The platform split lives in exactly one place: `ShmMap`.** Do not add `OperatingSystem.IsWindows()` branches
  elsewhere — if a new OS difference appears, it belongs in `ShmMap`.
- **Respect the read-in-place contract.** A span handed to a caller is valid only until `Advance*()`. Don't add
  APIs that retain or leak those spans; if you need to keep bytes, copy out (`TryReadCopy` is the model).
- **Barriers, not locks, on the hot path.** Publication ordering is release/acquire (`Volatile.Write` /
  `Volatile.Read`) on cursors and slot stamps. Preserve that; don't introduce locks into `TryWrite`/`TryRead`.
- **Bounded waits.** Every blocking call takes a `CancellationToken` and uses bounded internal waits so a missed
  signal degrades to a later wakeup, never a hang.
- **Zero-alloc where it's claimed.** If a path advertises zero allocation, add (or keep) a `[MemoryDiagnoser]`
  benchmark that proves 0 B/op.

## Tests

Add a test for any behavior change. The bar is *cross-process-faithful*: many tests spin a real server thread and
a real client over a real shared region, and some run distinct producer threads to model separate processes.
Prefer that over mocking the transport — the transport is the thing under test.

If you fix a correctness bug, pin it with a test that **fails before** your fix.

## Pull requests

- One focused change per PR; keep the diff reviewable.
- Green build + tests on both platforms (CI runs Windows and Linux).
- Describe *why*, not just *what* — especially for anything touching memory ordering, the ring protocol, or the
  platform split.
- Update the relevant doc under `docs/` if you change observable behavior, and add a line to
  [CHANGELOG.md](CHANGELOG.md).

## Reporting bugs

See [SECURITY.md](SECURITY.md) for anything with a safety/exploit angle. For ordinary bugs, open an issue with a
minimal repro — ideally a small program using the public API that demonstrates the problem.
