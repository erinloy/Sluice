---
name: Bug report
about: Something doesn't behave as documented
title: ''
labels: bug
assignees: ''
---

**What happened**
A clear description of the bug.

**Minimal repro**
A small program using the public API that demonstrates it (the smaller the better):

```csharp
// …
```

**Expected vs actual**
What you expected, and what occurred instead.

**Environment**
- OS: (Windows / Linux distro, container?)
- .NET version:
- Sluice version / commit:
- Topology in use: (RPC / topic / peer / frame channel / gossip / fusion)
- Delivery mode (if multicast): reliable / lossy

**Anything else**
Stack traces, `Dropped` counts, `/dev/shm` size, concurrency details — whatever's relevant.
