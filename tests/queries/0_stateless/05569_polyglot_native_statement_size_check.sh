#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POLY="--enable_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql --max_query_size 64"

# The client rejects an oversized polyglot query before tokenizing it, because the whole remaining
# input is transpiled at once. A statement that the polyglot dialect takes as native ClickHouse SQL
# (`SET ROLE NONE`, the `SET <setting>` shorthand) ends at its own semicolon, though, so a script may
# exceed `max_query_size` as a whole as long as each such statement fits.
echo "--- native statements in a script larger than max_query_size (expect: 1 x5) ---"
$CLICKHOUSE_CLIENT $POLY --multiquery -q "SET ROLE NONE; SET dialect = 'clickhouse'; SELECT 1; SELECT 1; SELECT 1; SELECT 1; SELECT 1"
echo "--- setting shorthand (expect: 1 x5) ---"
$CLICKHOUSE_CLIENT $POLY --multiquery -q "SET optimize_aggregation_in_order; SET dialect = 'clickhouse'; SELECT 1; SELECT 1; SELECT 1; SELECT 1; SELECT 1"
echo "--- clickhouse-local (expect: 1 x5) ---"
$CLICKHOUSE_LOCAL $POLY --multiquery -q "SET ROLE NONE; SET dialect = 'clickhouse'; SELECT 1; SELECT 1; SELECT 1; SELECT 1; SELECT 1"

# An oversized foreign query is still rejected with the dedicated error.
echo "--- oversized foreign query is rejected ---"
$CLICKHOUSE_CLIENT $POLY --multiquery -q "SET ROLE NONE; SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d, 5 AS e, 6 AS f, 7 AS g, 8 AS h, 9 AS i" 2>&1 | grep -om1 "counts towards max_query_size"
