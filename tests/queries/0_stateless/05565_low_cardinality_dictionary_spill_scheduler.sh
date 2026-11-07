#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_spill_scheduler"
TRACE_FILE=$(mktemp "${CLICKHOUSE_TMP}/dictionary_spill_scheduler.XXXXXX")
trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"; rm -f "${TRACE_FILE}"' EXIT
CLICKHOUSE_CLIENT_TRACE=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=trace/g")

${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 3, max_insert_block_size = 3;

    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        id UInt64,
        key LowCardinality(String),
        value UInt64
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS
        index_granularity = 1,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;

    SYSTEM STOP MERGES ${TABLE_NAME};

    INSERT INTO ${TABLE_NAME} VALUES (0, 'shared', 10), (2, 'shared', 30);
    INSERT INTO ${TABLE_NAME} VALUES (1, 'shared', 20);

    SELECT throwIf(
        count() != 2 OR countIf(part_type = 'Wide') != 2
            OR countIf(rows = 2) != 1 OR countIf(rows = 1) != 1,
        'Expected two Wide parts with interleaved rows for dictionary switches')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

# Sorting one-row blocks by `id` makes the single producer see dictionaries A, B, A.
# Without the adaptive memory spill scheduler, the switches move the producer to
# dictionary shards, and the shard state pre-merges with the initial local state.
# The scheduler spills only the producer-local table, so with it (and with both
# threshold-based spill settings zero) the producer must keep aggregating into its own
# table: no dictionary-shard state is created, so there is nothing to pre-merge.
run_aggregation()
{
    local scheduler="$1"
    local max_bytes_before_external_group_by="$2"

    ${CLICKHOUSE_CLIENT_TRACE} -q "
        SELECT key, count(), sum(value), groupArraySorted(10)(value)
        FROM
        (
            SELECT key, value
            FROM ${TABLE_NAME}
            ORDER BY id
        )
        GROUP BY key
        ORDER BY key
        SETTINGS
            max_threads = 1,
            max_block_size = 1,
            preferred_block_size_bytes = 0,
            optimize_read_in_order = 1,
            query_plan_remove_redundant_sorting = 0,
            optimize_aggregation_in_order = 0,
            enable_adaptive_aggregator = 0,
            collect_hash_table_stats_during_aggregation = 0,
            max_rows_to_group_by = 0,
            enable_adaptive_memory_spill_scheduler = ${scheduler},
            max_bytes_before_external_group_by = ${max_bytes_before_external_group_by},
            max_bytes_ratio_before_external_group_by = 0,
            group_by_two_level_threshold = 0,
            group_by_two_level_threshold_bytes = 0;
    " 2>"${TRACE_FILE}" || {
        query_status=$?
        cat "${TRACE_FILE}" >&2
        return "$query_status"
    }

    awk '
        /Aggregator: Merged [0-9]+ single-dictionary variants in [0-9]+ dictionary groups by index into [0-9]+ variants:/ {
            sub(/^.*Aggregator: /, "")
            sub(/, [0-9]+ workers,.*$/, "")
            print
        }
    ' "${TRACE_FILE}"
}

echo "without the spill scheduler"
run_aggregation 0 1073741824

echo "with the spill scheduler"
run_aggregation 1 0
