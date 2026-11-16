#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./correlated_scalar_aggregate_to_window.lib
. "$CUR_DIR"/correlated_scalar_aggregate_to_window.lib

# `optimize_correlated_scalar_aggregate_to_window` must not change any answer for joins that leave the subquery
# not rewritten. The other cases are in `05385_correlated_scalar_aggregate_to_window_not_rewritten`.

create_tables t u tp

check "PASTE JOIN" "SELECT n.number AS n, o.id AS id, (SELECT sum(v) FROM tp AS s WHERE s.k = o.k) AS c FROM (SELECT number FROM numbers(4)) AS n PASTE JOIN tp AS o WHERE n.number >= 0 AND o.k < 5 ORDER BY n"
check "condition on the key that also depends on the other table" "SELECT o.k AS k, o.v AS v, u.w AS w, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o INNER JOIN u ON o.k = u.k WHERE o.k < u.w ORDER BY k, v, w"

run_all

echo "--- materialized view reading its source table in the subquery ---"
# The subquery reads only the inserted block, as without the rewrite.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE mv_src (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mv_dst (k Int32, v Int32, c Nullable(Int64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO mv_src VALUES (1, 100), (2, 200);
CREATE MATERIALIZED VIEW mv TO mv_dst AS SELECT o.k AS k, o.v AS v, (SELECT sum(v) FROM mv_src AS s WHERE s.k = o.k) AS c FROM mv_src AS o WHERE o.k < 3;
INSERT INTO mv_src SETTINGS optimize_correlated_scalar_aggregate_to_window = 1 VALUES (1, 1), (2, 2);
SELECT * FROM mv_dst ORDER BY k;
"
