#!/usr/bin/env bash
# Tags: no-fasttest, use_jemalloc
# no-fasttest: requires USE_EMBEDDED_COMPILER, which the fast-test image disables.
# use_jemalloc: measures the dedicated jemalloc arena for JIT allocations.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every query compiles a filter with different literals. The compiled expression cache holds one entry,
# so the memory used by JIT compilation must stay flat instead of growing with each compiled query.

JIT_ARENA_BYTES="SELECT sum(curregs * size) FROM system.jemalloc_arena_bins WHERE purpose = 'jit' FORMAT TSV;"

{
    echo "CREATE TABLE t (key UInt64, value UInt64) ENGINE = Memory;"
    echo "INSERT INTO t SELECT number, number FROM numbers(1000);"
    for i in $(seq 1 4); do
        printf "SELECT count() FROM t WHERE "
        for j in $(seq 1 150); do
            [ "$j" -gt 1 ] && printf " OR "
            printf "(key = %d AND value > %d)" $((i * 100000 + j)) $((i * 100000 + j * 7))
        done
        echo " SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0 FORMAT Null;"
        [ "$i" -eq 1 ] && echo "$JIT_ARENA_BYTES"
    done
    echo "$JIT_ARENA_BYTES"
} > "${CLICKHOUSE_TMP}/queries.sql"

$CLICKHOUSE_LOCAL --queries-file "${CLICKHOUSE_TMP}/queries.sql" -- --compiled_expression_cache_elements_size=1 < /dev/null \
    | tr '\n' ' ' | awk '{ growth = $2 - $1; print (growth < 2000000 ? "ok" : "JIT arena grew by " growth " bytes") }'
