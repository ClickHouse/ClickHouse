#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: the data can be inserted on a different node

# A format setting declared with the legacy `s3queue_` prefix in the SETTINGS
# clause must reach the parser (issue #125459).

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_PATH="test/${CLICKHOUSE_DATABASE}_$(basename "$0" .sh)"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/${S3_PATH}/data.csv', format = CSV)
    VALUES ('hdr'), ('1'), ('2'), ('3')
    SETTINGS s3_truncate_on_insert = 1
"

$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.queue (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/${S3_PATH}/*.csv', 'CSV')
    SETTINGS
        keeper_path = '/clickhouse/${CLICKHOUSE_DATABASE}/$(basename "$0" .sh)',
        mode = 'unordered',
        after_processing = 'keep',
        s3queue_input_format_csv_skip_first_lines = 1
"

$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM ${CLICKHOUSE_DATABASE}.queue
    SETTINGS stream_like_engine_allow_direct_select = 1
"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}.queue SYNC"
