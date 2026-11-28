#!/usr/bin/env bash
# A `FileLog` batch that is still in flight at shutdown lands in a `Buffer` of another database, and its rows must reach the destination.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}/logs"
seq 1 4 > "${WORKING_DIR}/logs/data.csv"

# `clickhouse-local` resolves the path of `FileLog` against the current directory.
cd "${WORKING_DIR}" || exit 1
LOCAL="${CLICKHOUSE_LOCAL} --path ${WORKING_DIR}/data"

# The database of the `Buffer` sorts before the database of the source. The view takes 2 seconds for the batch,
# so the session ends while the batch is in flight.
$LOCAL -q "
CREATE DATABASE a_dst;
CREATE DATABASE b_src;
CREATE TABLE a_dst.dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE a_dst.buf (x UInt64) ENGINE = Buffer(a_dst, dst, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
CREATE TABLE b_src.fl (x UInt64) ENGINE = FileLog('logs/', 'CSV') SETTINGS poll_max_batch_size = 4, max_block_size = 4;
CREATE MATERIALIZED VIEW b_src.mv TO a_dst.buf AS SELECT x FROM b_src.fl WHERE sleepEachRow(0.5) = 0;
SELECT sleepEachRow(1) FROM numbers(1) FORMAT Null;
" < /dev/null

# If the batch had not started yet, a later session starts it. Each session waits longer than the previous one.
for i in {1..10}
do
    count=$($LOCAL -q "SELECT count() FROM a_dst.dst" < /dev/null)
    [ "$count" = 4 ] && break
    $LOCAL -q "SELECT sleepEachRow(1) FROM numbers($i) FORMAT Null SETTINGS max_block_size = 1" < /dev/null
done
echo "$count"

cd "${CUR_DIR}" || exit 1
rm -rf "${WORKING_DIR}"
