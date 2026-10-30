#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A fresh clickhouse-local caches the reader zone America/New_York before the file's writer zone US/Eastern,
# an alias of it, is first looked up in the process.
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
$CLICKHOUSE_LOCAL -q "SELECT toDateTime('2020-06-01 12:00:00', 'UTC') AS t FORMAT ORC SETTINGS output_format_orc_writer_time_zone_name = 'US/Eastern'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT toUnixTimestamp(t) FROM file('$FILE', ORC) SETTINGS input_format_orc_reader_time_zone_name = 'America/New_York'"
rm -f "$FILE"
