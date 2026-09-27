#!/usr/bin/env bash
# End-to-end suites for the WebSocket SDKs (UI -> gateway -> Brahmaputra):
#
#   clients/ws/test.sh                 # every suite
#   clients/ws/test.sh js browser      # some of them
#
# Suites: js (core client, stores, Svelte adapter), browser (React, Vue
# and Angular apps in Chromium via Playwright), dart, flutter (widget tests).
# Starts its own broker; each suite starts the gateway processes it needs.
# A suite whose toolchain is missing is reported as SKIP.
#
# Environment: CHROMIUM (browser executable; default /opt/pw-browsers/chromium
# or Playwright's own), FLUTTER (flutter binary), BROKER_PORT.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
ALL=(js browser dart flutter)
SELECTED=("$@")
[[ ${#SELECTED[@]} -eq 0 ]] && SELECTED=("${ALL[@]}")

(cd "$ROOT" && cargo build --release --locked -p brahmaputra-server -p brahmaputra-ws-gateway) || exit 1
export GW_BIN="$ROOT/target/release/brahmaputra-ws-gateway"
export BRP_HOST=127.0.0.1
export BRP_PORT="${BROKER_PORT:-19092}"

DATA="$(mktemp -d "${TMPDIR:-/tmp}/brahmaputra-ws-sdk.XXXXXX")"
RUST_LOG=warn "$ROOT/target/release/brahmaputra-server" --data-dir "$DATA/data" \
  --default-partitions 4 --port "$BRP_PORT" --http-port $((BRP_PORT + 1)) >"$DATA/broker.log" 2>&1 &
BROKER=$!
trap 'kill $BROKER 2>/dev/null; wait $BROKER 2>/dev/null; rm -rf "$DATA"' EXIT
for _ in $(seq 1 100); do (exec 3<>"/dev/tcp/127.0.0.1/$BRP_PORT") 2>/dev/null && break; sleep 0.1; done

npm_install() {
  if [[ -f package-lock.json ]]; then npm ci --no-audit --no-fund --loglevel=error
  else npm install --no-audit --no-fund --loglevel=error; fi
}

build_js() {
  for pkg in js react vue angular; do
    (cd "$HERE/$pkg" && npm_install && npx --no-install tsc -p tsconfig.json) || return 1
  done
}

FLUTTER="${FLUTTER:-$(command -v flutter || true)}"
DART="$(command -v dart || true)"
[[ -z "$DART" && -n "$FLUTTER" ]] && DART="$(dirname "$FLUTTER")/dart"

run_suite() {
  case "$1" in
    js)
      command -v node >/dev/null || return 3
      build_js && (cd "$HERE/js" && node --test --test-reporter=spec test/*.test.mjs) ;;
    browser)
      command -v node >/dev/null || return 3
      if [[ -z "${CHROMIUM:-}" && -x /opt/pw-browsers/chromium ]]; then export CHROMIUM=/opt/pw-browsers/chromium; fi
      build_js && (cd "$HERE/e2e" && npm_install && node build.mjs &&
        node --test --test-concurrency=1 --test-reporter=spec browser.test.mjs) ;;
    dart)
      [[ -n "$DART" ]] || return 3
      (cd "$HERE/dart" && "$DART" pub get && "$DART" analyze --fatal-infos && "$DART" test -r expanded) ;;
    flutter)
      [[ -n "$FLUTTER" ]] || return 3
      (cd "$HERE/flutter" && "$FLUTTER" pub get && "$FLUTTER" analyze && "$FLUTTER" test -r expanded) ;;
    *) echo "unknown suite $1 (known: ${ALL[*]})" >&2; return 2 ;;
  esac
}

declare -A RESULT
failed=0
for suite in "${SELECTED[@]}"; do
  echo
  echo "=================== $suite ==================="
  run_suite "$suite"
  case $? in
    0) RESULT[$suite]=PASS ;;
    3) RESULT[$suite]=SKIP ;;
    *) RESULT[$suite]=FAIL; failed=1
       echo "---- last 50 broker log lines ----"; tail -n 50 "$DATA/broker.log" ;;
  esac
done

echo
echo "=================== summary ==================="
for suite in "${SELECTED[@]}"; do printf '%-8s %s\n' "$suite" "${RESULT[$suite]}"; done
exit $failed
