#!/bin/sh
set -eu
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
MANIFEST="$ROOT/results/manifest.json"
VALIDATOR="$ROOT/../.github/scripts/validate_manifest.py"
if [ ! -f "$VALIDATOR" ]; then
  VALIDATOR="/Users/nguyenhuyvu/projects/stra-ta/.github/scripts/validate_manifest.py"
fi
if [ -f "$MANIFEST" ]; then
  python3 "$VALIDATOR" "$MANIFEST"
  echo "manifest OK: $MANIFEST"
else
  echo "no manifest at $MANIFEST (skipping)"
fi
# stale deterministic checks
if [ -f "$ROOT/tools/check_evidence.py" ]; then
  python3 "$ROOT/tools/check_evidence.py" --root "$ROOT"
fi
echo "evidence checks passed"
