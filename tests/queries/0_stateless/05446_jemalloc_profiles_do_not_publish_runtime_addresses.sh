#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-fasttest, no-debug, no-llvm-coverage
# NOTE: jemalloc is disabled under sanitizers

# Heap profiles hold main binary offsets (or 0) instead of runtime addresses, and no memory map.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Addresses of `@` lines and of the symbol section of a heap profile.
ADDRESSES="arrayMap(x -> reinterpretAsUInt64(reverse(unhex(leftPad(substring(x, 3), 16, '0')))),
    if(startsWith(line, '@'), arrayFilter(x -> x != '', splitByChar(' ', substring(line, 2))), extractAll(line, '^0x[0-9a-fA-F]+')))"

# The Memory table keeps sampled allocations alive, so the profiles are not empty.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05446 (s String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --jemalloc_enable_profiler=1 -q "INSERT INTO t_05446 SELECT repeat(repeat('a', 100), 1000000)"

for format in raw symbolized; do
    echo "$format"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT
            countIf(line LIKE 'MAPPED_LIBRARIES%'),
            countIf(arrayExists(x -> x > (SELECT max(address_end) FROM system.symbols), ${ADDRESSES})),
            countIf(arrayExists(x -> x != 0, ${ADDRESSES})) > 0
        FROM system.jemalloc_profile_text
        SETTINGS jemalloc_profile_text_output_format = '$format', jemalloc_profile_text_symbolize_with_inline = 0,
            allow_introspection_functions = 1"
done

echo "sampled allocations"
${CLICKHOUSE_CLIENT} -q "
    SELECT
        countIf(arrayExists(x -> x > (SELECT max(address_end) FROM system.symbols), trace)),
        countIf(arrayExists(x -> x != 0, trace)) > 0
    FROM system.jemalloc_sampled_allocations
    SETTINGS allow_introspection_functions = 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_05446"

