#!/usr/bin/env bash
# Tags: no-fasttest

# An unbounded stream from a `FileLog` table checks at every poll that the file still has the contents it has read.
# A file rewritten in place with a longer last record (`a` becomes `ab`) after the first bytes that are compared keeps
# the first bytes and the start of the last record, but not its newline: the read position would fall inside the new
# record, so the file is read again from the beginning instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "$dir"
mkdir -p "$dir"

# The first 256 bytes of the file: a record of 255 bytes with its newline.
head_record=$(printf 'x%.0s' $(seq 1 255))

$STREAMING_CLIENT -q "CREATE TABLE file_log (s String) ENGINE = FileLog('$dir/*.log', 'LineAsString') SETTINGS poll_timeout_ms = 100"

printf '%s\na\n' "$head_record" > "$dir/u.log"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT length(s), s FROM file_log STREAM")
read_until "$fifo" "	a"
# The same inode (`>` truncates the file in place).
printf '%s\nab\nc\n' "$head_record" > "$dir/u.log"
read_until "$fifo" "	c"
cleanup "$fifo" "$pid" > /dev/null

$STREAMING_CLIENT -q "DROP TABLE file_log"
rm -rf "$dir"
