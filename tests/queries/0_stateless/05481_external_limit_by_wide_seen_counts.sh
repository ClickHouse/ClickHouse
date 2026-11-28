#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Seen runs store the counts of a spilled `LIMIT BY` clamped to the window end, in the narrowest type that
# holds it: `UInt16` for `LIMIT 300 BY`, `UInt32` for `LIMIT 70000 BY`. A few keys repeat more often than the
# window end before the spill, then many unique keys grow the hash table past the threshold, then the repeated
# keys come back. Every row of a repeated key after the spill ranks past the window, so each repeated key
# keeps exactly the window's rows; a count truncated to a narrower type would keep more. `toUInt64` keeps one
# key type in all phases: without it, `multiIf` returns a `Variant`, whose differently typed values are
# different keys. Each process has its own events.
function limit_by_query()
{
    local window=$1 keys=$2 repeats=$3 unique=$4
    local repeated=$((keys * repeats))
    ${CLICKHOUSE_LOCAL} --query "
        SELECT count(), countIf(k < $keys)
        FROM
        (
            SELECT k
            FROM
            (
                SELECT toUInt64(multiIf(
                    number < $repeated, number % $keys,
                    number < $repeated + $unique, number,
                    (number - $repeated - $unique) % $keys)) AS k
                FROM numbers($repeated * 2 + $unique)
            )
            LIMIT $window BY k
        )
        SETTINGS max_threads = 1, max_block_size = 65536, max_untracked_memory = 0,
            max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = 100663296;
        SELECT value > 0 FROM system.events WHERE event = 'ExternalLimitByWritePart';"
}

limit_by_query 300 1000 1000 3000000
limit_by_query 70000 2 100000 3000000
