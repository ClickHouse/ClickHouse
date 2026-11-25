#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database

# The positions of a streaming read of a `FileLog` table, mostly for an incremental refresh:
# - a file without a complete record is still a file of the table after it is renamed to a name that does not match the glob;
# - the same for a rotated file that was truncated to empty;
# - a file truncated and written again past the read position is read from the beginning;
# - a query that stops the read early still advances the cursor, and `LIMIT` is rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "$dir"
mkdir -p "$dir"

refresh()
{
    $STREAMING_CLIENT -q "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv; SELECT '$1', arraySort(groupArray(k)) FROM tgt"
}

$STREAMING_CLIENT -q "
    CREATE TABLE file_log (k UInt64) ENGINE = FileLog('$dir/*.log', 'CSV') SETTINGS poll_timeout_ms = 100;
    CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
"

echo "--- an incomplete record is completed after the file is rotated"
printf '1' > "$dir/app.log"
$STREAMING_CLIENT -q "CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT k FROM file_log"
refresh round1
mv "$dir/app.log" "$dir/app.log.1"
printf '\n' >> "$dir/app.log.1"
refresh round2
refresh round3

echo "--- a rotated file is truncated to empty and gets new records"
: > "$dir/app.log.1"
refresh round4
printf '2\n' >> "$dir/app.log.1"
refresh round5
refresh round6

echo "--- a file is truncated and written again past the position of the cursor"
printf '10\n' > "$dir/r.log"
refresh round7
printf '20\n30\n' > "$dir/r.log"
refresh round8
refresh round9
# The new contents start with the old ones, but the last record read is not there any more.
printf '20\n31\n32\n33\n' > "$dir/r.log"
refresh round10
refresh round11
$STREAMING_CLIENT -q "DROP TABLE mv"
rm "$dir"/*

echo "--- unbounded: a file is truncated and written again past the read position"
printf '1\n' > "$dir/u.log"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT k FROM file_log STREAM")
read_until "$fifo" "1"
printf '2\n3\n' > "$dir/u.log"
read_until "$fifo" "2"
read_until "$fifo" "3"
cleanup "$fifo" "$pid" > /dev/null
$STREAMING_CLIENT -q "DROP TABLE file_log"
rm "$dir"/*

echo "--- a query that stops the read early"
seq 1 100 > "$dir/e.log"
$STREAMING_CLIENT -q "
    CREATE TABLE file_log (k UInt64) ENGINE = FileLog('$dir/*.log', 'CSV') SETTINGS max_block_size = 1;
    TRUNCATE TABLE tgt;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT k FROM file_log LIMIT 1; -- { serverError NOT_IMPLEMENTED }
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT DISTINCT toUInt64(1) AS k FROM file_log SETTINGS max_threads = 1;
"
refresh round1
refresh round2
seq 101 200 >> "$dir/e.log"
refresh round3
refresh round4

$STREAMING_CLIENT -q "DROP TABLE mv"
$STREAMING_CLIENT -q "DROP TABLE tgt"
$STREAMING_CLIENT -q "DROP TABLE file_log"
rm -rf "$dir"
