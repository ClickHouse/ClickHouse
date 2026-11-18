#!/usr/bin/env bash
# Tags: use-musl
# Tag use-musl: `__syscall_cp_asm` is a musl function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A thread blocked in a cancellable system call, such as a condition variable wait, has musl's assembly stub
# `__syscall_cp_asm` as its innermost frame. The stub must have a symbol size, otherwise `SymbolIndex` skips
# its symbol and the innermost frame of most threads in `system.stack_trace` and in `trace_log` is unnamed.
# `ConfigReloader` and `AsyncMetrics` spend almost all their time in such a wait. Only these threads are read,
# because each snapshot signals every thread it reads, which is slow with many copies of the test in parallel.
query="
SELECT countIf(addressToSymbol(trace[1]) = '__syscall_cp_asm') > 0
FROM system.stack_trace
WHERE thread_name IN ('AsyncMetrics', 'ConfigReloader')
SETTINGS allow_introspection_functions = 1"

# A thread can be caught awake, or signalling it can time out, so a negative result is retried.
for _ in {1..30}
do
    result=$($CLICKHOUSE_CLIENT -q "$query")
    [ "$result" = "1" ] && break
done

echo "$result"
