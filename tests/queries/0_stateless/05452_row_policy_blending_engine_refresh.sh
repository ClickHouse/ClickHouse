#!/usr/bin/env bash
# a refresh copies its merging target, so a database policy accepted once does not stop it

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR ENGINE = SummingMergeTree ORDER BY k EMPTY AS SELECT number AS k, 1 AS v FROM numbers(3)"
$CLICKHOUSE_CLIENT --allow_suspicious_row_policies_with_blending_engines 1 -q "CREATE ROW POLICY p_db ON $CLICKHOUSE_DATABASE.* USING k > 0 TO ALL"

$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM mv ORDER BY k"

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_db ON $CLICKHOUSE_DATABASE.*"
