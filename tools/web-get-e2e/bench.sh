#!/usr/bin/env bash
# Phase-0/1 carrier bench matrix: runs run.sh bench cells across
# carrier x method x edge x RTT and writes a markdown summary.
#   tools/web-get-e2e/bench.sh <prefix>
# prefix distinguishes before/after runs, e.g. `base` and `after`.
# Optional TELEMT_E2E_BIN pins a prebuilt test binary (see run.sh).
# Optional BENCH_FILTER (extended regex on the cell label) reruns a subset;
# BENCH_HEADER_BYTES sets the header payload of the geth/gethf cells.
set -euo pipefail
cd "$(dirname "$0")"

PREFIX="${1:?bench prefix required (e.g. base, after)}"
HEADER_BYTES="${BENCH_HEADER_BYTES:-40000}"

# Cells: method edge-port rtt-ms. 443/8443 are the GET-only edges (h1.1/h2);
# 9443/10443 are the open edges used only by POST baseline cells.
# Full matrix at RTT 0 and 50, GET-only cells at RTT 150 per the plan.
# `geth` is the GET carrier with header payload behind an edge with raised
# request-head buffers; `gethf` is the same page behind stock nginx buffers,
# where the page must fall back to URL-only fragments.
CELLS=(
  "get 443 0" "get 8443 0" "post 9443 0" "post 10443 0"
  "get 443 50" "get 8443 50" "post 9443 50" "post 10443 50"
  "get 443 150" "get 8443 150"
  "geth 443 0" "geth 8443 0" "geth 443 50" "geth 8443 50"
  "geth 443 150" "geth 8443 150"
  "gethf 443 50" "gethf 8443 50"
)

FAILED=()
for carrier in https https-lanes; do
  for cell in "${CELLS[@]}"; do
    read -r method port rtt <<< "$cell"
    label="$PREFIX-$carrier-$method-p$port-r$rtt"
    if [ -n "${BENCH_FILTER:-}" ] && ! [[ "$label" =~ $BENCH_FILTER ]]; then
      continue
    fi
    case "$method" in
      geth) wire=get headers="$HEADER_BYTES" edge_headers=large ;;
      gethf) wire=get headers="$HEADER_BYTES" edge_headers=default ;;
      *) wire="$method" headers=0 edge_headers=large ;;
    esac
    echo "=== bench cell $label ==="
    if ! TELEMT_WEB_E2E_METHOD="$wire" \
      TELEMT_WEB_E2E_GET_HEADER_BYTES="$headers" \
      TELEMT_E2E_NGINX_HEADERS="$edge_headers" \
      bash ./run.sh "$carrier" bench "$wire" "$rtt" "https://proxy.example.com:$port" "$label"; then
      FAILED+=("$label")
    fi
  done
done

# wslpath occasionally fails transiently under load; retry briefly.
WIN_DIR=""
for _ in 1 2 3 4 5; do
  WIN_DIR=$(wslpath -w "$PWD" 2>/dev/null) && [ -n "$WIN_DIR" ] && break
  sleep 1
done
[ -n "$WIN_DIR" ] || { echo "wslpath failed for $PWD" >&2; exit 1; }
node.exe "$WIN_DIR\\bench-report.mjs" "$PREFIX" "$WIN_DIR\\artifacts" || true
echo "bench summary: artifacts/bench-$PREFIX.md"
if [ "${#FAILED[@]}" -gt 0 ]; then
  echo "failed cells: ${FAILED[*]}" >&2
  exit 1
fi
