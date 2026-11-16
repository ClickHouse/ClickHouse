#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database

# STREAM from a `FileLog` table reads the files without consuming them.
# Its cursor is {file name: <offset of the last read record>}; a file name may contain dots.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "$dir"
mkdir -p "$dir"

printf '1,a\n2,b\n3,c\n' > "$dir/a.log"
printf '10,x\n20,y\n' > "$dir/b.log"

$STREAMING_CLIENT -q "CREATE TABLE file_log (k UInt64, v String) ENGINE = FileLog('$dir', 'CSV') SETTINGS poll_timeout_ms = 100"

echo "--- bounded, no cursor"
$STREAMING_CLIENT -q "SELECT _filename, _offset, k, v FROM file_log STREAM BOUNDED ORDER BY ALL"

echo "--- bounded, with cursor"
$STREAMING_CLIENT -q "SELECT _filename, k FROM file_log STREAM BOUNDED CURSOR {'a.log': 4, 'b.log': 5} ORDER BY ALL"

echo "--- the STREAM does not consume the records"
$STREAMING_CLIENT -q "SELECT count() FROM file_log SETTINGS stream_like_engine_allow_direct_select = 1"

echo "--- a cursor outside a file"
$STREAMING_CLIENT -q "SELECT k FROM file_log STREAM BOUNDED CURSOR {'a.log': 1000}" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

echo "--- unbounded, with cursor"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT _filename, k FROM file_log STREAM CURSOR {'a.log': 8, 'b.log': 5}")
printf '4,d\n' >> "$dir/a.log"
read_until "$fifo" "4"
# An incomplete record is not read until it is complete.
printf '5,' >> "$dir/a.log"
printf '30,z\n' > "$dir/c.log"
read_until "$fifo" "30"
printf 'e\n' >> "$dir/a.log"
read_until "$fifo" "5"
cleanup "$fifo" "$pid" > /dev/null

echo "--- incremental refresh"
refresh()
{
    $STREAMING_CLIENT -q "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv; SELECT '$1', count(), sum(k) FROM tgt"
}
$STREAMING_CLIENT -q "
    CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT k FROM file_log;
"
refresh round1
printf '6,f\n' >> "$dir/a.log"
refresh round2
# A file truncated in place is read from the beginning.
: > "$dir/b.log"
printf '7,g\n' >> "$dir/b.log"
refresh round3
refresh round4
$STREAMING_CLIENT -q "DROP TABLE mv"

$STREAMING_CLIENT -q "DROP TABLE file_log"
rm -rf "$dir"
