#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database

# A streaming read of a `FileLog` table identifies the files by inode:
# - a file renamed to a name that does not match the glob of the table is still read (`app.log` -> `app.log.1` for `*.log`);
# - the cursor of an incremental refresh does not keep the previous names of a renamed file;
# - a file truncated to empty is read from the beginning when it gets new records.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "$dir"
mkdir -p "$dir"

printf '1\n' > "$dir/app.log"
# This file does not match the glob and was never read: it is not a file of the table.
printf '1000\n' > "$dir/other.txt"

$STREAMING_CLIENT -q "CREATE TABLE file_log (k UInt64) ENGINE = FileLog('$dir/*.log', 'CSV') SETTINGS poll_timeout_ms = 100"

echo "--- unbounded: a rotated file is read after it was drained"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT _filename, k FROM file_log STREAM")
read_until "$fifo" "1"
mv "$dir/app.log" "$dir/app.log.1"
printf '50\n' > "$dir/app.log"
read_until "$fifo" "50"
printf '7\n' >> "$dir/app.log.1"
read_until "$fifo" "7"
cleanup "$fifo" "$pid" > /dev/null
rm "$dir/app.log"
mv "$dir/app.log.1" "$dir/app.log"

echo "--- incremental refresh: two renames of a file"
refresh()
{
    $STREAMING_CLIENT -q "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv; SELECT '$1', count(), sum(k) FROM tgt"
}
$STREAMING_CLIENT -q "
    CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT k FROM file_log;
"
refresh round1
mv "$dir/app.log" "$dir/app.log.1"
printf '2\n' >> "$dir/app.log.1"
refresh round2
mv "$dir/app.log.1" "$dir/app.log.2"
printf '3\n' >> "$dir/app.log.2"
refresh round3
refresh round4

echo "--- incremental refresh: a file truncated to empty"
printf '10\n' > "$dir/t.log"
refresh round5
: > "$dir/t.log"
refresh round6
printf '20\n30\n' >> "$dir/t.log"
refresh round7
refresh round8

$STREAMING_CLIENT -q "DROP TABLE mv"
$STREAMING_CLIENT -q "DROP TABLE file_log"
rm -rf "$dir"
