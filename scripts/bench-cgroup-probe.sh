#!/bin/sh
# Run inside an isolated benchmark container. The caller supplies a unique
# stop marker, starts the workload only after the first sample, then creates
# the marker and waits for the final sample. No packages or elevated container
# privileges are needed. CPU is cumulative, so subsecond phases remain visible.
set -eu
stop_marker=${1:?expected a unique /tmp/brahma-bench-probe-*.stop path}
case "$stop_marker" in /tmp/brahma-bench-probe-*.stop) ;; *) exit 2 ;; esac
trap 'rm -f "$stop_marker"' 0
sample_interval=${2:-0.05}
cgroup_root=${CGROUP_ROOT:-/sys/fs/cgroup}
if [ ! -r "$cgroup_root/cpu.stat" ] || [ ! -r "$cgroup_root/memory.current" ]; then
    printf 'cgroup v2 CPU/memory accounting is unavailable\n' >&2
    exit 3
fi
snapshot() {
    usage_usec=
    while read -r key value rest; do
        if [ "$key" = usage_usec ]; then usage_usec=$value; break; fi
    done < "$cgroup_root/cpu.stat"
    [ -n "$usage_usec" ] || return 3
    read -r memory_total < "$cgroup_root/memory.current"
    inactive_file=0
    while read -r key value rest; do
        if [ "$key" = inactive_file ]; then inactive_file=$value; break; fi
    done < "$cgroup_root/memory.stat"
    # Same Linux working-set convention as Docker stats on cgroup v2:
    # https://docs.docker.com/reference/cli/docker/container/stats/
    memory_working_set=$((memory_total - inactive_file))
    if [ "$memory_working_set" -lt 0 ]; then memory_working_set=0; fi
    read -r uptime_seconds rest < /proc/uptime
    printf '%s,%s,%s,%s\n' "$uptime_seconds" "$usage_usec" "$memory_working_set" "$memory_total"
}
printf 'uptime_seconds,cpu_usage_usec,memory_working_set_bytes,memory_total_bytes\n'
samples=0
while :; do
    snapshot
    [ ! -e "$stop_marker" ] || break
    samples=$((samples + 1))
    # A crashed caller must not leave a sampler running indefinitely.
    if [ "$samples" -ge 24000 ]; then
        printf 'benchmark resource sampler exceeded its sample limit\n' >&2
        exit 4
    fi
    sleep "$sample_interval"
done
