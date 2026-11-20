#!/usr/bin/env bash
# Tags: no-fasttest, use-vectorscan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With non-constant needles every row of the block compiles its own vectorscan database inside one
# `multiMatchAny` call. The deadline must stop the call, not only the next block: this stops in about a
# second, while running the whole block takes more than two minutes on a release build.

check()
{
    local name=$1 predicate=$2
    if timeout 60 ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM numbers(1500000) WHERE ${predicate}
        SETTINGS max_block_size = 1500000, max_threads = 1, max_execution_time = 1
    " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
    then
        echo "${name}: stopped at the deadline"
    else
        echo "${name}: still running after 60 seconds"
    fi
}

check multiMatchAny "multiMatchAny(toString(number), ['^' || toString(number)])"
check multiMatchAllIndices "length(multiMatchAllIndices(toString(number), ['^' || toString(number)])) > 0"
