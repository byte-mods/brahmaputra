#!/usr/bin/env bash
# Run every client driver's end-to-end suite against one live broker.
#
#   clients/run-e2e.sh                 # every driver
#   clients/run-e2e.sh go python c     # just these
#
# Starts a private broker (4 default partitions) on BROKER_PORT (default
# 19092) with its data under a temp dir, runs each driver's `test.sh HOST
# PORT` in turn, prints a summary table, and exits non-zero if any driver
# failed. A driver whose toolchain is not installed is reported as SKIP,
# not as a pass. Set BROKER_ADDR=host:port to use a broker you already run.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLIENTS="$ROOT/clients"
ALL=(rust go nodejs python java dotnet cpp c php ruby erlang elixir)
SELECTED=("$@")
[[ ${#SELECTED[@]} -eq 0 ]] && SELECTED=("${ALL[@]}")

# The toolchain each driver needs; missing means SKIP.
declare -A NEEDS=(
  [rust]=cargo [go]=go [nodejs]=node [python]=python3 [java]=javac
  [dotnet]=dotnet [cpp]=cmake [c]=cc [php]=php [ruby]=ruby
  [erlang]=erlc [elixir]=mix
)

BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null && wait "$BROKER_PID" 2>/dev/null
  [[ -n "${DATA_DIR:-}" ]] && rm -rf "$DATA_DIR"
}
trap cleanup EXIT

if [[ -n "${BROKER_ADDR:-}" ]]; then
  HOST="${BROKER_ADDR%:*}"
  PORT="${BROKER_ADDR##*:}"
else
  HOST=127.0.0.1
  PORT="${BROKER_PORT:-19092}"
  HTTP_PORT="${BROKER_HTTP_PORT:-18092}"
  SERVER="$ROOT/target/release/brahmaputra-server"
  if [[ ! -x "$SERVER" ]]; then
    echo "building brahmaputra-server (release)..."
    (cd "$ROOT" && cargo build --release -p brahmaputra-server) || exit 1
  fi
  DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/brahmaputra-clients-e2e.XXXXXX")"
  "$SERVER" --data-dir "$DATA_DIR/data" --default-partitions 4 \
    --port "$PORT" --http-port "$HTTP_PORT" > "$DATA_DIR/broker.log" 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 50); do
    (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null && break
    sleep 0.2
  done
  if ! (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null; then
    echo "broker did not start; log:" >&2
    cat "$DATA_DIR/broker.log" >&2
    exit 1
  fi
fi
echo "broker at $HOST:$PORT"

declare -A RESULT
declare -A SUMMARY
failed=0
for lang in "${SELECTED[@]}"; do
  if [[ -z "${NEEDS[$lang]:-}" ]]; then
    echo "unknown driver: $lang (known: ${ALL[*]})" >&2
    exit 2
  fi
  if ! command -v "${NEEDS[$lang]}" >/dev/null 2>&1; then
    RESULT[$lang]=SKIP
    SUMMARY[$lang]="${NEEDS[$lang]} not installed"
    continue
  fi
  echo
  echo "=================== $lang ==================="
  log="$(mktemp)"
  if [[ "$lang" == rust ]]; then
    cmd=(cargo run --quiet --release --manifest-path "$ROOT/Cargo.toml"
      -p brahmaputra-client --example manual_test -- "$HOST" "$PORT")
  else
    cmd=(bash "$CLIENTS/$lang/test.sh" "$HOST" "$PORT")
  fi
  if "${cmd[@]}" 2>&1 | tee "$log"; [[ ${PIPESTATUS[0]} -eq 0 ]]; then
    RESULT[$lang]=PASS
  else
    RESULT[$lang]=FAIL
    failed=1
  fi
  SUMMARY[$lang]="$(grep -E '[0-9]+ passed, [0-9]+ failed' "$log" | tail -1)"
  rm -f "$log"
done

echo
echo "=================== summary ==================="
for lang in "${SELECTED[@]}"; do
  printf '%-8s %-5s %s\n' "$lang" "${RESULT[$lang]}" "${SUMMARY[$lang]}"
done
exit $failed
