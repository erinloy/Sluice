<!-- Thanks for contributing! Keep PRs focused and reviewable. See CONTRIBUTING.md. -->

## What & why

<!-- What changes, and the reasoning — especially for anything touching memory ordering,
     the ring protocol, the platform split, or delivery semantics. -->

## Checklist

- [ ] `dotnet test Sluice.slnx -c Release` is green on Windows **and** Linux (CI covers both)
- [ ] Behavior changes have a test; bug fixes have a test that **fails before** the fix
- [ ] Zero-alloc claims are backed by a `[MemoryDiagnoser]` benchmark (0 B/op)
- [ ] The platform split stays in `ShmMap` only (no new `OperatingSystem.IsWindows()` branches elsewhere)
- [ ] Read-in-place contract preserved (no span retained past `Advance*()`)
- [ ] Relevant `docs/` guide updated; `CHANGELOG.md` has an entry
