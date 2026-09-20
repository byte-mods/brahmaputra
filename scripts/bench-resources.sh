#!/usr/bin/env bash
# Sourced by benchmark harnesses that provide ROOT and docker_run. Sampling
# brackets the complete client phase, including client startup/exit dispatch.
# A container's CPU includes its broker, clients and the probe process.
RESOURCE_PIDS=()
RESOURCE_CONTAINERS=()
RESOURCE_FILES=()
RESOURCE_OUTPUT=''
RESOURCE_STOP=''

benchmark_now_ms() { node "$ROOT/scripts/bench-clock.cjs"; }

start_resource_sampling() {
  local out="$1" container file ready index deadline
  shift
  command -v node >/dev/null || { printf 'Resource sampling requires Node.js\n' >&2; return 1; }
  (( ${#RESOURCE_PIDS[@]} == 0 )) || { printf 'Resource sampler already active\n' >&2; return 1; }
  (( $# > 0 )) || return 1
  RESOURCE_OUTPUT="$out"
  RESOURCE_STOP="/tmp/brahma-bench-probe-$BASHPID-$RANDOM.stop"
  RESOURCE_CONTAINERS=("$@")
  RESOURCE_FILES=()
  for container in "$@"; do
    file="$out.$container.cgroup.csv"
    RESOURCE_FILES+=("$file")
    docker_run exec -i "$container" sh -s -- "$RESOURCE_STOP" "${RESOURCE_SAMPLE_INTERVAL:-0.05}" \
      < "$ROOT/scripts/bench-cgroup-probe.sh" > "$file" 2> "$file.stderr" &
    RESOURCE_PIDS+=("$!")
  done
  deadline=$(( $(benchmark_now_ms) + 20000 ))
  while (( $(benchmark_now_ms) < deadline )); do
    ready=1
    for index in "${!RESOURCE_FILES[@]}"; do
      if ! kill -0 "${RESOURCE_PIDS[$index]}" 2>/dev/null; then
        printf 'Resource probe exited before workload: %s\n' "${RESOURCE_CONTAINERS[$index]}" >&2
        stop_resource_sampling || true
        return 1
      fi
      [[ $(awk 'NR == 2 { print "ready"; exit }' "${RESOURCE_FILES[$index]}") == ready ]] || ready=0
    done
    (( ready == 1 )) && return 0
    sleep 0.02
  done
  printf 'Resource probes did not become ready\n' >&2
  stop_resource_sampling || true
  return 1
}

stop_resource_sampling() {
  (( ${#RESOURCE_PIDS[@]} > 0 )) || return 0
  local container pid failed=0
  local -a stops=()
  # Stop all nodes concurrently so multi-node windows end together.
  for container in "${RESOURCE_CONTAINERS[@]}"; do
    docker_run exec "$container" sh -c ': > "$1"' sh "$RESOURCE_STOP" >/dev/null 2>&1 &
    stops+=("$!")
  done
  for pid in "${stops[@]}"; do wait "$pid" || failed=1; done
  if (( failed )); then
    for pid in "${RESOURCE_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  fi
  for pid in "${RESOURCE_PIDS[@]}"; do wait "$pid" || failed=1; done
  RESOURCE_PIDS=()
  if (( failed )); then
    printf 'Resource probe failed; no resource figures will be reported\n' >&2
    printf 'NA NA NA NA\n' > "$RESOURCE_OUTPUT"
    return 1
  fi
  node "$ROOT/scripts/bench-resource-summary.cjs" "$RESOURCE_OUTPUT.resources.json" \
    "${RESOURCE_FILES[@]}" > "$RESOURCE_OUTPUT"
}

summarize_resource_samples() { cat "$1"; }

resource_report() {
  printf '\nWall-clock phase rates, where reported, use monotonic elapsed time.\n'
  printf '\nCPU time comes from cumulative cgroup v2 counters; memory is working set\n'
  printf '(memory.current minus inactive_file), sampled every %s seconds.\n' "${RESOURCE_SAMPLE_INTERVAL:-0.05}"
  printf 'Sampling brackets the complete client phase, including startup/exit dispatch\n'
  printf 'and the probe itself. Multi-node metrics use the common observation window.\n'
  printf 'CPU percentages are core-equivalents (100%% = one busy core); peaks are\n'
  printf 'observed/interpolated sample peaks. CPU time compares total work, while\n'
  printf 'average CPU describes utilization during each system\047s own run.\n'
  node "$ROOT/scripts/bench-resource-summary.cjs" --report "$1"
}
