#!/usr/bin/env bash
# Build, rewrite, and run the Coyote systematic-concurrency tests for Sluice's lock-free rings.
#
# Coyote takes control of the scheduler and explores producer/consumer interleavings exhaustively (within a
# step bound), proving the ring never loses, reorders, duplicates, or tears a message under any of them. The
# assemblies must be rewritten before the scheduler can intercept them — this does the full cycle.
#
# Requires the Coyote CLI:  dotnet tool install --global Microsoft.Coyote.CLI --version 1.7.11
set -euo pipefail

ITERATIONS="${1:-1000}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here/../tests/Sluice.Concurrency"

if ! command -v coyote >/dev/null 2>&1; then
  export PATH="$HOME/.dotnet/tools:$PATH"
fi
if ! command -v coyote >/dev/null 2>&1; then
  echo "The 'coyote' CLI was not found. Install it with: dotnet tool install --global Microsoft.Coyote.CLI --version 1.7.11" >&2
  exit 1
fi

echo "== build =="
dotnet build Sluice.Concurrency.csproj -c Release --nologo

echo "== rewrite =="
coyote rewrite rewrite.coyote.json

dll="bin/Release/net8.0/Sluice.Concurrency.dll"
tests=(
  "Sluice.Concurrency.RingConcurrencyTests.Spsc_ring_never_loses_reorders_or_tears_a_message"
  "Sluice.Concurrency.RingConcurrencyTests.Read_in_place_slot_is_not_reclaimed_before_AdvanceRead"
)

failed=0
for t in "${tests[@]}"; do
  echo "== test: $t =="
  coyote test "$dll" -m "$t" -i "$ITERATIONS" || failed=1
done

if [ "$failed" -ne 0 ]; then
  echo "Coyote found a bug — see the replayable .schedule trace above." >&2
  exit 1
fi
echo "All Coyote tests passed (0 bugs)."
