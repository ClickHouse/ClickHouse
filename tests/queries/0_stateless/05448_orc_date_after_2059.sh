#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Date` is the unsigned number of days, and the dates after 2059-09-18 (more than 32767 days) used
# to be written to ORC as negative numbers, which read back as the dates before 1970.
DATA_FILE="$CLICKHOUSE_TMP/05448_${CLICKHOUSE_DATABASE}.orc"

$CLICKHOUSE_LOCAL -q "SELECT number AS n, toDate(d) AS d FROM values('number UInt64, d String', (0, '1970-01-01'), (1, '2059-09-17'), (2, '2059-09-18'), (3, '2100-01-01'), (4, '2149-06-06')) FORMAT ORC" > "$DATA_FILE"

$CLICKHOUSE_LOCAL -q "SELECT n, d, toTypeName(d) FROM file('$DATA_FILE', ORC) ORDER BY n"
$CLICKHOUSE_LOCAL -q "SELECT n, d FROM file('$DATA_FILE', ORC, 'n UInt64, d Date') ORDER BY n"

# The statistics are correct as well, so the filter pushdown keeps the rows.
$CLICKHOUSE_LOCAL -q "SELECT n FROM file('$DATA_FILE', ORC, 'n UInt64, d Date') WHERE d > '2060-01-01' ORDER BY n SETTINGS input_format_orc_filter_push_down = 1"

rm "$DATA_FILE"
