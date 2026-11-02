#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

# An `ordered` S3Queue must never commit a file after a larger file of the same bucket:
# that commit fails after the insert, so `after_processing` is skipped and the rest of
# the batch is read and inserted again.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_processed()
{
    local keeper_path=$1
    local expected=$2
    local processed
    for _ in {1..240}; do
        processed=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.s3queue_metadata_cache WHERE zookeeper_path = '${keeper_path}' AND status = 'Processed'") || break
        if [ "$processed" -ge "$expected" ]; then
            break
        fi
        sleep 0.5
    done
}

function run_arm()
{
    local arm=$1
    local settings=$2
    local filter=${3:-}
    local second_server=${4:-}
    local s3_path="test/${CLICKHOUSE_DATABASE}_05483_${arm}"
    local keeper_path="/clickhouse/${CLICKHOUSE_DATABASE}/05483_${arm}"
    local queue_settings="keeper_path = '${keeper_path}', mode = 'ordered', polling_min_timeout_ms = 50, polling_max_timeout_ms = 200, ${settings}"

    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${s3_path}/f{_partition_id}.csv', format = CSV, structure = 'id UInt64')
        PARTITION BY intDiv(id, 5)
        SELECT number FROM numbers(100)
        SETTINGS s3_truncate_on_insert = 1
    "

    $CLICKHOUSE_CLIENT --send_logs_level=error -q "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_${arm} (id UInt64) ENGINE = MergeTree ORDER BY id;
        CREATE TABLE ${CLICKHOUSE_DATABASE}.queue_${arm} (id UInt64)
        ENGINE = S3Queue('http://localhost:11111/${s3_path}/*.csv', 'CSV')
        SETTINGS ${queue_settings};
        CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv_${arm} TO ${CLICKHOUSE_DATABASE}.dst_${arm}
        AS SELECT id FROM ${CLICKHOUSE_DATABASE}.queue_${arm} ${filter};
    "

    if [ -n "$second_server" ]; then
        # Another server on the same `keeper_path`, started while the first one holds the buckets.
        wait_processed "$keeper_path" 1
        $CLICKHOUSE_CLIENT --send_logs_level=error -q "
            CREATE TABLE ${CLICKHOUSE_DATABASE}.queue_${arm}2 (id UInt64)
            ENGINE = S3Queue('http://localhost:11111/${s3_path}/*.csv', 'CSV')
            SETTINGS ${queue_settings};
            CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv_${arm}2 TO ${CLICKHOUSE_DATABASE}.dst_${arm}
            AS SELECT id FROM ${CLICKHOUSE_DATABASE}.queue_${arm}2 ${filter};
        "
    fi

    local left_query="SELECT count(DISTINCT _path) FROM s3(s3_conn, url = 'http://localhost:11111/${s3_path}/*.csv', format = CSV, structure = 'id UInt64') SETTINGS use_cache_for_count_from_files = 0"

    # With the bug, the file committed out of order never becomes `Processed`.
    wait_processed "$keeper_path" 20

    $CLICKHOUSE_CLIENT -q "SELECT '${arm}', count(), uniqExact(id) FROM ${CLICKHOUSE_DATABASE}.dst_${arm}"

    if [[ "$settings" == *"'delete'"* ]]; then
        local left
        for _ in {1..60}; do
            left=$($CLICKHOUSE_CLIENT -q "$left_query") || break
            if [ "$left" = 0 ]; then
                break
            fi
            sleep 0.5
        done
        echo "${arm} objects left: $($CLICKHOUSE_CLIENT -q "$left_query")"
    fi

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE ${CLICKHOUSE_DATABASE}.mv_${arm};
        DROP TABLE ${CLICKHOUSE_DATABASE}.queue_${arm} SYNC;
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.mv_${arm}2;
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.queue_${arm}2 SYNC;
        DROP TABLE ${CLICKHOUSE_DATABASE}.dst_${arm};
    "
}

# #114801: several sources claim files concurrently, one file per INSERT.
run_arm A "after_processing = 'delete', processing_threads_num = 8, buckets = 1, list_objects_batch_size = 2, max_processed_files_before_commit = 1"
# `parallel_inserts = 1`, four buckets.
run_arm B "parallel_inserts = 1, processing_threads_num = 4, buckets = 0, after_processing = 'keep'"
# `parallel_inserts = 1`, one bucket.
run_arm C "parallel_inserts = 1, processing_threads_num = 4, buckets = 1, after_processing = 'keep'"
# The second server can claim nothing while the first one holds both buckets: it must wait for the next poll,
# not run empty INSERTs in a loop until the buckets are released.
run_arm D "processing_threads_num = 2, buckets = 2, max_processed_files_before_commit = 1, after_processing = 'keep'" "WHERE sleepEachRow(0.1) = 0" 1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS background_schedule_pool_log"
$CLICKHOUSE_CLIENT -q "SELECT 'D second server polls, long tasks', count() > 1, countIf(duration_ms >= 3000) FROM system.background_schedule_pool_log WHERE database = currentDatabase() AND table = 'queue_D2'"
