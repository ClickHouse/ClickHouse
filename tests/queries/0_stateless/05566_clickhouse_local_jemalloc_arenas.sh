#!/usr/bin/env bash
# Tags: no-fasttest, no-tsan, no-asan, no-msan
# ^ jemalloc is not built in these configurations

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# CI sets MALLOC_CONF, which keeps per-CPU arenas.
unset MALLOC_CONF

cpus=$(python3 -c 'import os; print(len(os.sched_getaffinity(0)))')
arenas=$(( (cpus + 7) / 8 )); (( arenas < 2 )) && arenas=2; (( arenas > cpus )) && arenas=$cpus
last_cpu=$(python3 -c 'import os; print(max(os.sched_getaffinity(0)))')
mode="SELECT extract(stats, 'opt.percpu_arena: \"[a-z]+\"') FROM system.jemalloc_stats"
bin_dir="${CLICKHOUSE_TMP}/bin-${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${bin_dir}"
mkdir -p "${bin_dir}"
binary=$(readlink -f "$(command -v "${CLICKHOUSE_BINARY}")")
ln -s "${binary}" "${bin_dir}/clickhouse-local"
ln -s "${binary}" "${bin_dir}/chl"

${CLICKHOUSE_LOCAL} -q "SELECT extract(stats, 'opt.percpu_arena: \"[a-z]+\"'), extract(stats, 'opt.narenas: [0-9]+') = 'opt.narenas: $arenas' FROM system.jemalloc_stats"
${CLICKHOUSE_BINARY} local --tmp -q "$mode"
${CLICKHOUSE_BINARY} --local --tmp -q "$mode"
"${bin_dir}/clickhouse-local" --tmp -q "$mode"
"${bin_dir}/chl" --tmp -q "$mode"
# The implicit form (no `local` argument) keeps per-CPU arenas.
${CLICKHOUSE_BINARY} --tmp -q "$mode"
taskset --cpu-list "${last_cpu}" ${CLICKHOUSE_LOCAL} -q "SELECT extract(stats, 'opt.narenas: [0-9]+') FROM system.jemalloc_stats"
MALLOC_CONF=abort_conf:true,abort:false ${CLICKHOUSE_LOCAL} -q "$mode"
${CLICKHOUSE_LOCAL} -q "SELECT count() FROM system.warnings WHERE message LIKE 'jemalloc per-CPU arenas are disabled%'"
MALLOC_CONF=percpu_arena:disabled,narenas:2 ${CLICKHOUSE_LOCAL} -q "SELECT count() FROM system.warnings WHERE message LIKE 'jemalloc per-CPU arenas are disabled%'"
${CLICKHOUSE_LOCAL} -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t SELECT number FROM numbers(100000); SELECT sum(x) FROM t"
rm -rf "${bin_dir}"
