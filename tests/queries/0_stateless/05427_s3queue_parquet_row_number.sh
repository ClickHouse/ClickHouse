#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

# `S3Queue` reads its objects without a `FormatFilterInfo`; `_row_number` must still be
# materialized from the row numbers the Parquet reader attaches to its chunks.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_05427"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data.parquet', format = Parquet)
    SELECT number * 10 AS x FROM numbers(3)
    SETTINGS s3_truncate_on_insert = 1
"

$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.parquet', 'Parquet')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/05427_s3queue',
        mode = 'unordered',
        after_processing = 'keep'
"

$CLICKHOUSE_CLIENT -q "
    SELECT x, _row_number FROM ${CLICKHOUSE_DATABASE}.queue ORDER BY x
    SETTINGS stream_like_engine_allow_direct_select = 1, input_format_parquet_use_native_reader_v3 = 1
"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}.queue SYNC"
