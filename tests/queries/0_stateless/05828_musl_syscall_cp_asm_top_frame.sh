#!/usr/bin/env bash
# Tags: use-musl
# Tag use-musl: the frames below are musl functions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A thread blocked in a cancellable system call, such as a condition variable wait, has a musl syscall stub
# as its innermost frame: the assembly `__syscall_cp_asm` when `pthread_cancel` is linked into the binary,
# otherwise `__syscall_cp_c`. Every stub must have a symbol size, otherwise `SymbolIndex` skips its symbol
# and the innermost frame of most threads in `system.stack_trace` and in `trace_log` is unnamed.
# `ConfigReloader` and `AsyncMetrics` spend almost all their time in such a wait. Only these threads are read,
# because each snapshot signals every thread it reads, which is slow with many copies of the test in parallel.
query="
SELECT count() > 0 AND countIf(addressToSymbol(trace[1]) = '') = 0
FROM system.stack_trace
WHERE thread_name IN ('AsyncMetrics', 'ConfigReloader')
SETTINGS allow_introspection_functions = 1"

# A thread can be caught in the vDSO, whose frames are stored as 0, or signalling it can time out, leaving
# an empty trace, so a negative result is retried.
for _ in {1..30}
do
    result=$($CLICKHOUSE_CLIENT -q "$query")
    [ "$result" = "1" ] && break
done

echo "$result"
