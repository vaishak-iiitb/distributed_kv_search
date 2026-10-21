#!/bin/sh
# Runs multi_shard_test repeatedly and reports how many runs passed.
#
#   tools/flake_loop.sh            10 runs, nothing recorded
#   tools/flake_loop.sh 20         20 runs, nothing recorded
#   tools/flake_loop.sh 20 record  20 runs, then append ONE row to
#                                  docs/phase3/flake-loop-log.md
#
# The pass/fail counts come from the real exit code of each run. Nothing in
# the recorded row is typed by hand. The binary is found automatically: the
# most recently built multi_shard_test under the repo root is used.

set -u

runs="${1:-10}"
mode="${2:-}"

case "$runs" in
    ''|*[!0-9]*) echo "runs must be a positive integer, got: $runs" >&2; exit 2 ;;
esac
if [ "$runs" -lt 1 ]; then echo "runs must be at least 1" >&2; exit 2; fi

root="$(cd "$(dirname "$0")/.." && pwd)"

bin="$(find "$root" -name multi_shard_test -type f -perm -u+x -not -path '*/CMakeFiles/*' -exec ls -t {} + 2>/dev/null | head -n 1)"
if [ -z "$bin" ]; then
    echo "multi_shard_test was not found under $root. Build it first with cmake --build." >&2
    exit 2
fi
bindir="$(dirname "$bin")"
echo "using binary: $bin"

pass=0
fail=0
codes=""
i=1
while [ "$i" -le "$runs" ]; do
    start="$(date +%s)"
    ( cd "$bindir" && ./multi_shard_test > "flake_run_$i.log" 2>&1 )
    code=$?
    end="$(date +%s)"
    if [ "$code" -eq 0 ]; then pass=$((pass + 1)); else fail=$((fail + 1)); fi
    codes="$codes $code"
    echo "run $i/$runs: exit=$code ($((end - start))s) log: $bindir/flake_run_$i.log"
    i=$((i + 1))
    sleep 2
done

echo "RESULT: $pass/$runs passed, $fail failed (exit codes:$codes)"

if [ "$mode" = "record" ]; then
    log="$root/docs/phase3/flake-loop-log.md"
    if [ ! -f "$log" ]; then
        {
            echo "# multi_shard_test flake-loop log"
            echo
            echo "Each row is appended by tools/flake_loop.sh from real exit codes."
            echo
            echo "| date (UTC) | git commit | tree | runs | passed | failed | machine |"
            echo "| --- | --- | --- | --- | --- | --- | --- |"
        } > "$log"
    fi
    commit="$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    if git -C "$root" diff --quiet 2>/dev/null; then tree="clean"; else tree="uncommitted changes"; fi
    echo "| $(date -u +%Y-%m-%d\ %H:%M) | $commit | $tree | $runs | $pass | $fail | $(uname -sm) |" >> "$log"
    echo "recorded one row in $log"
fi

if [ "$fail" -eq 0 ]; then exit 0; else exit 1; fi