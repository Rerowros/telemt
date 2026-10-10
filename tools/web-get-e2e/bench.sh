#!/usr/bin/env bash
# Phase-0/1 carrier bench matrix: runs run.sh bench cells across
# carrier x method x edge x RTT and writes a markdown summary.
#   tools/web-get-e2e/bench.sh <prefix>
# prefix distinguishes before/after runs, e.g. `base` and `after`.
# Optional TELEMT_E2E_BIN pins a prebuilt test binary (see run.sh).
set -euo pipefail
cd "$(dirname "$0")"

PREFIX="${1:?bench prefix required (e.g. base, after)}"

# Cells: method edge-port rtt-ms. 443/8443 are the GET-only edges (h1.1/h2);
# 9443/10443 are the open edges used only by POST baseline cells.
# Full matrix at RTT 0 and 50, GET-only cells at RTT 150 per the plan.
CELLS=(
  "get 443 0" "get 8443 0" "post 9443 0" "post 10443 0"
  "get 443 50" "get 8443 50" "post 9443 50" "post 10443 50"
  "get 443 150" "get 8443 150"
)

FAILED=()
for carrier in https https-lanes; do
  for cell in "${CELLS[@]}"; do
    read -r method port rtt <<< "$cell"
    label="$PREFIX-$carrier-$method-p$port-r$rtt"
    echo "=== bench cell $label ==="
    if ! TELEMT_WEB_E2E_METHOD="$method" bash ./run.sh "$carrier" bench "$method" "$rtt" "https://proxy.example.com:$port" "$label"; then
      FAILED+=("$label")
    fi
  done
done

WIN_DIR=$(wslpath -w "$PWD")
node.exe "$WIN_DIR\\bench-report.mjs" "$PREFIX" "$WIN_DIR\\artifacts" || true
echo "bench summary: artifacts/bench-$PREFIX.md"
if [ "${#FAILED[@]}" -gt 0 ]; then
  echo "failed cells: ${FAILED[*]}" >&2
  exit 1
fi
