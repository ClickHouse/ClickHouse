#!/usr/bin/env bash
set -euo pipefail

# Paths
FLAKE_NIX="flake.nix"
GENERATE_SCRIPT="utils/nix/generate_submodule_inputs.sh"
TMP_FILE="$(mktemp)"
FINAL_FILE="$(mktemp)"

# 1. Strip from first contrib- line to end
sed '/^    *contrib-/,${d}' "$FLAKE_NIX" > "$TMP_FILE"

# 2. Append new contrib lines
"$GENERATE_SCRIPT" >> "$TMP_FILE"

# 3. Append closing braces
echo "  };" >> "$TMP_FILE"
echo "}" >> "$TMP_FILE"

# 4. Check if content differs
if cmp -s "$TMP_FILE" "$FLAKE_NIX"; then
  echo "✅ flake.nix is already up to date."
else
  mv "$TMP_FILE" "$FLAKE_NIX"
  echo "✅ Updated contrib inputs in flake.nix."
fi

# Cleanup
rm -f "$FINAL_FILE" 2>/dev/null || true
