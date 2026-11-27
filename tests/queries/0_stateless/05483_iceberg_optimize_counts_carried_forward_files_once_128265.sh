#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# https://github.com/ClickHouse/ClickHouse/issues/128265
# The manifest list of every snapshot also lists the manifests of the earlier snapshots. `OPTIMIZE` must still
# write the `total-records`, `total-data-files` and `total-files-size` of its snapshot from the live files (the
# trivial `count()` reads `total-records`), and read each position delete file once per data file it applies to.
# The earlier snapshots keep their totals.
#
# The cloud build routes `OPTIMIZE` through a different code path, so the effects of the compaction are not
# asserted there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2"

# $1: label, $2: statements that build the history of `{table}`, $3: expected reads per delete file (optional),
# $4: expected `total-records` of the snapshots, $5: expected `total-data-files` of the snapshots.
run_case()
{
    local label="$1" history="$2" expected_reads="$3" expected_totals="$4" expected_files="$5"
    local table="t_${CLICKHOUSE_DATABASE}_${label}"
    local table_path="${USER_FILES_PATH:?}/${table}/"
    local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}_${RANDOM}"

    $CH --query "CREATE TABLE ${table} (x Int64) ENGINE = IcebergLocal('${table_path}', 'Parquet')"
    $CH --query "${history//\{table\}/${table}}"

    local delete_files
    delete_files=$($CH --query "
        SELECT file_path FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '${table}' AND content = 'POSITION_DELETE'
        ORDER BY sequence_number")

    local optimize_err
    optimize_err=$($CH --allow_experimental_iceberg_compaction=1 --enable_blob_storage_log_for_read_operations=1 \
        --query_id "${query_id}" --query "OPTIMIZE TABLE ${table}" 2>&1)

    if [[ "${IS_CLOUD}" = "1" ]]; then
        echo "${label} 2 2"
        echo "${label} total-records: ${expected_totals}"
        echo "${label} total-data-files: ${expected_files}"
        echo "${label} total-files-size is the size of the data files: 1"
        [[ -n "${expected_reads}" ]] && echo "${label} reads per delete file: ${expected_reads}"
    else
        [[ -n "${optimize_err}" ]] && echo "${label} FAIL: OPTIMIZE failed: ${optimize_err}"
        echo "${label}" \
            "$($CH --query "SELECT count() FROM ${table} SETTINGS optimize_trivial_count_query = 1")" \
            "$($CH --query "SELECT count() FROM ${table} SETTINGS optimize_trivial_count_query = 0")"
        echo "${label} total-records:" "$($CH --query "
            SELECT arraySort(groupArray(summary['total-records']))
            FROM system.iceberg_history
            WHERE database = currentDatabase() AND table = '${table}'")"
        echo "${label} total-data-files:" "$($CH --query "
            SELECT arraySort(groupArray(summary['total-data-files']))
            FROM system.iceberg_history
            WHERE database = currentDatabase() AND table = '${table}'")"
        echo "${label} total-files-size is the size of the data files:" "$($CH --query "
            SELECT
                (SELECT toInt64(summary['total-files-size']) FROM system.iceberg_history
                 WHERE database = currentDatabase() AND table = '${table}' AND is_current_ancestor
                     AND snapshot_id NOT IN (SELECT parent_id FROM system.iceberg_history
                         WHERE database = currentDatabase() AND table = '${table}' AND is_current_ancestor))
                = (SELECT sum(file_size_in_bytes) FROM system.iceberg_files
                   WHERE database = currentDatabase() AND table = '${table}' AND content = 'DATA')")"
        if [[ -n "${expected_reads}" ]]; then
            $CH --query "SYSTEM FLUSH LOGS blob_storage_log"
            local reads=()
            for delete_file in ${delete_files}; do
                reads+=("$($CH --query "
                    SELECT count() FROM system.blob_storage_log
                    WHERE event_date >= yesterday() AND query_id = '${query_id}' AND event_type = 'Read'
                        AND remote_path LIKE '%/$(basename "${delete_file}")'")")
            done
            echo "${label} reads per delete file: ${reads[*]}"
        fi
    fi

    $CH --query "DROP TABLE ${table} SYNC"
    rm -rf "${table_path}"
}

# The issue: the rows of the second append's parent were counted a second time (3).
run_case appended_parent "
    INSERT INTO {table} VALUES (1), (2);
    INSERT INTO {table} VALUES (3);
    ALTER TABLE {table} DELETE WHERE x = 1" "" "['2','2','3','3']" "['1','2','2','2']"

# A delete-only snapshot between the two appends.
run_case delete_parent "
    INSERT INTO {table} VALUES (1), (2);
    ALTER TABLE {table} DELETE WHERE x = 1;
    INSERT INTO {table} VALUES (3)" "" "['2','2','2','3']" "['1','1','2','2']"

# Each delete file applies to one data file: two reads (the schema and the rows) each.
run_case delete_file_reads "
    INSERT INTO {table} VALUES (1);
    INSERT INTO {table} VALUES (2);
    INSERT INTO {table} VALUES (3);
    INSERT INTO {table} VALUES (4);
    ALTER TABLE {table} DELETE WHERE x = 1;
    ALTER TABLE {table} DELETE WHERE x = 2" "2 2" "['1','2','2','3','4','4','4']" "['1','2','2','3','4','4','4']"
