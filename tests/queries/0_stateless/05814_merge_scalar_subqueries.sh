#!/usr/bin/env bash
# Tests for `optimize_merge_scalar_subqueries`:
# 1. Result and outward-contract preservation, and single-scan verification via `system.query_log`.
# 2. Table/column `SELECT` grants and row policies under merging.
# 3. Differential checks: every merged query must return the same results and types as with merging disabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'SQL'
-- Merging of aggregate scalar subqueries over the same table: results and outward query contracts must not change.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS ms_t;
DROP TABLE IF EXISTS ms_t2;
DROP TABLE IF EXISTS ms_r;
DROP TABLE IF EXISTS ms_key;
-- No key columns, so subqueries over `ms_t` are merged.
CREATE TABLE ms_t (k UInt32, a Int64, b Float64, n Nullable(Int64), arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ms_t2 (k UInt32, v Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_t SELECT number, number % 100 - 50, number * 1.5, if(number % 2 = 0, NULL, number), [number % 7] FROM numbers(1000);
INSERT INTO ms_t2 SELECT number, number * 3 FROM numbers(100);

SELECT
    (SELECT count() FROM ms_t WHERE k < 100),
    (SELECT sum(a) FROM ms_t WHERE k < 100),
    (SELECT avg(b) FROM ms_t WHERE k >= 100 AND k < 200),
    (SELECT min(a) FROM ms_t WHERE k >= 100 AND k < 300),
    (SELECT max(a) FROM ms_t WHERE k >= 100);
SELECT
    (SELECT count() FROM ms_t WHERE k > 100000),
    (SELECT sum(a) FROM ms_t WHERE k > 100000),
    (SELECT avg(b) FROM ms_t WHERE k > 100000),
    (SELECT min(a) FROM ms_t WHERE k > 100000);

-- Not merged: different tables, correlated, ORDER BY, IN subquery.
SELECT (SELECT count() FROM ms_t WHERE k < 100), (SELECT count() FROM ms_t2 WHERE k < 10);
SELECT k, (SELECT max(a) FROM ms_t t2 WHERE t2.k = ms_t.k), (SELECT min(a) FROM ms_t WHERE k > 1) FROM ms_t ORDER BY k LIMIT 3;
SELECT (SELECT k FROM ms_t WHERE k > 5 ORDER BY k LIMIT 1), (SELECT count() FROM ms_t WHERE k < 100);
SELECT (SELECT count() FROM ms_t WHERE k IN (SELECT k FROM ms_t2)), (SELECT count() FROM ms_t WHERE k < 100);

-- `count(expr)`, `FINAL`, non-`UInt8` conditions and subquery `SETTINGS`.
SELECT (SELECT count(n) FROM ms_t WHERE k < 500), (SELECT count(n) FROM ms_t WHERE k < 1000);
CREATE TABLE ms_r (k UInt32, v UInt32) ENGINE = ReplacingMergeTree ORDER BY tuple();
INSERT INTO ms_r VALUES (1, 1);
INSERT INTO ms_r VALUES (1, 2);
SELECT (SELECT count() FROM ms_r FINAL WHERE v > 0), (SELECT count() FROM ms_r WHERE v > 0);
SELECT (SELECT count() FROM ms_t WHERE k), (SELECT count() FROM ms_t WHERE n), (SELECT sum(a) FROM ms_t WHERE b);
SELECT (SELECT count() FROM ms_t WHERE k > 0 SETTINGS additional_table_filters = {'ms_t': 'k < 10'}), (SELECT count() FROM ms_t WHERE k > 0);

-- Aggregate arguments are not evaluated on rows of other subqueries, and aliases do not leak between them.
SELECT (SELECT sum(intDiv(100, a)) FROM ms_t WHERE a = 1), (SELECT count() FROM ms_t WHERE k < 2);
SELECT (SELECT sum(b AS a) FROM ms_t WHERE k < 10), (SELECT max(a) FROM ms_t WHERE k > 990);
SELECT (SELECT count() FROM ms_t WHERE (k < 10) AS c), (SELECT count() FROM ms_t WHERE (k > 990) AS c);

-- Result types, explicit aliases, and unaliased `_subquery_N` column names must be preserved.
SELECT toTypeName((SELECT count() FROM ms_t WHERE k < 2)), toTypeName((SELECT min(arr) FROM ms_t WHERE k < 3));
SELECT (SELECT count() FROM ms_t WHERE k < 10) AS x, (SELECT sum(a) FROM ms_t WHERE k < 20) AS y FORMAT TSVWithNames;
SELECT (SELECT count() FROM ms_t WHERE k < 10), (SELECT sum(a) FROM ms_t WHERE k < 20) FORMAT TSVWithNames;
SELECT exists(SELECT count() FROM ms_t WHERE k > 100000), (SELECT sum(a) FROM ms_t WHERE k > 1);
SELECT 10 IN (SELECT count() FROM ms_t WHERE k < 10), (SELECT sum(a) FROM ms_t WHERE k < 10);

SELECT (SELECT count() FROM ms_t WHERE k > 100000), (SELECT count() FROM ms_t WHERE k < 10)
SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT (SELECT count() FROM ms_t WHERE k < 100), (SELECT sum(a) FROM ms_t WHERE k < 100) SETTINGS enable_analyzer = 0;

-- Subqueries with non-shared key predicates keep per-subquery primary key analysis;
-- subqueries sharing a key predicate keep primary key analysis AND merge into one scan.
CREATE TABLE ms_key (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO ms_key SELECT number, number FROM numbers(65536);
SELECT (SELECT count() FROM ms_key WHERE k < 10), (SELECT sum(v) FROM ms_key WHERE k < 20) SETTINGS max_rows_to_read = 20000;
SELECT (SELECT count() FROM ms_key WHERE k < 10 AND v < 5), (SELECT sum(v) FROM ms_key WHERE k < 10 AND v >= 5)
SETTINGS max_rows_to_read = 20000, log_comment = '05814_shared_pk_merged';
SELECT (SELECT count() FROM ms_key WHERE k < 10 AND v < 5), (SELECT sum(v) FROM ms_key WHERE k < 10 AND v >= 5)
SETTINGS max_rows_to_read = 20000, log_comment = '05814_shared_pk_not_merged', optimize_merge_scalar_subqueries = 0;

-- Merged subqueries read the table once.
SELECT (SELECT count() FROM ms_t WHERE a < 0), (SELECT sum(b) FROM ms_t WHERE a > 10) SETTINGS log_comment = '05814_merged';
SELECT (SELECT count() FROM ms_t WHERE a < 0), (SELECT sum(b) FROM ms_t WHERE a > 10)
SETTINGS log_comment = '05814_not_merged', optimize_merge_scalar_subqueries = 0;

-- TPC-DS Q9 pattern: multiple `multiIf` buckets with `count(*)` and `avg(Decimal)` over a keyed `MergeTree` table.
DROP TABLE IF EXISTS ms_store_sales;
CREATE TABLE ms_store_sales
(
    ss_item_sk Int64,
    ss_ticket_number Int64,
    ss_quantity Nullable(Int64),
    ss_ext_discount_amt Nullable(Decimal(7, 2)),
    ss_net_paid Nullable(Decimal(7, 2))
) ENGINE = MergeTree PRIMARY KEY (ss_item_sk, ss_ticket_number);
INSERT INTO ms_store_sales SELECT number, number, (number % 100) + 1, toDecimal32(number % 50, 2), toDecimal32(number % 80, 2) FROM numbers(2000);

SELECT
    multiIf((SELECT count(*) FROM ms_store_sales WHERE ss_quantity >= 1 AND ss_quantity <= 20) > 100,
            (SELECT avg(ss_ext_discount_amt) FROM ms_store_sales WHERE ss_quantity >= 1 AND ss_quantity <= 20),
            (SELECT avg(ss_net_paid) FROM ms_store_sales WHERE ss_quantity >= 1 AND ss_quantity <= 20)) AS bucket1,
    multiIf((SELECT count(*) FROM ms_store_sales WHERE ss_quantity >= 21 AND ss_quantity <= 40) > 5000,
            (SELECT avg(ss_ext_discount_amt) FROM ms_store_sales WHERE ss_quantity >= 21 AND ss_quantity <= 40),
            (SELECT avg(ss_net_paid) FROM ms_store_sales WHERE ss_quantity >= 21 AND ss_quantity <= 40)) AS bucket2,
    multiIf((SELECT count(*) FROM ms_store_sales WHERE ss_quantity >= 41 AND ss_quantity <= 60) > 100,
            (SELECT avg(ss_ext_discount_amt) FROM ms_store_sales WHERE ss_quantity >= 41 AND ss_quantity <= 60),
            (SELECT avg(ss_net_paid) FROM ms_store_sales WHERE ss_quantity >= 41 AND ss_quantity <= 60)) AS bucket3
SETTINGS log_comment = '05814_tpcds_q9_merged';

SELECT
    multiIf((SELECT count(*) FROM ms_store_sales WHERE ss_quantity >= 1 AND ss_quantity <= 20) > 100,
            (SELECT avg(ss_ext_discount_amt) FROM ms_store_sales WHERE ss_quantity >= 1 AND ss_quantity <= 20),
            (SELECT avg(ss_net_paid) FROM ms_store_sales WHERE ss_quantity >= 1 AND ss_quantity <= 20)) AS bucket1,
    multiIf((SELECT count(*) FROM ms_store_sales WHERE ss_quantity >= 21 AND ss_quantity <= 40) > 5000,
            (SELECT avg(ss_ext_discount_amt) FROM ms_store_sales WHERE ss_quantity >= 21 AND ss_quantity <= 40),
            (SELECT avg(ss_net_paid) FROM ms_store_sales WHERE ss_quantity >= 21 AND ss_quantity <= 40)) AS bucket2,
    multiIf((SELECT count(*) FROM ms_store_sales WHERE ss_quantity >= 41 AND ss_quantity <= 60) > 100,
            (SELECT avg(ss_ext_discount_amt) FROM ms_store_sales WHERE ss_quantity >= 41 AND ss_quantity <= 60),
            (SELECT avg(ss_net_paid) FROM ms_store_sales WHERE ss_quantity >= 41 AND ss_quantity <= 60)) AS bucket3
SETTINGS log_comment = '05814_tpcds_q9_not_merged', optimize_merge_scalar_subqueries = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment,
    if(log_comment LIKE '05814_shared_pk%', read_rows < 10000,
        if(log_comment LIKE '05814_tpcds_q9%', read_rows <= 2500, read_rows < 1500))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN (
        '05814_merged', '05814_not_merged',
        '05814_shared_pk_merged', '05814_shared_pk_not_merged',
        '05814_tpcds_q9_merged', '05814_tpcds_q9_not_merged')
ORDER BY log_comment;

DROP TABLE ms_store_sales;
DROP TABLE ms_key;
DROP TABLE ms_t;
DROP TABLE ms_t2;
DROP TABLE ms_r;

-- Regression cases for settings, query modifiers, computed columns, views, aliases, temporary tables, PREWHERE, and set operations.
DROP TABLE IF EXISTS ms_regression;
DROP TABLE IF EXISTS ms_regression_source;
DROP TABLE IF EXISTS ms_regression_default;
DROP TABLE IF EXISTS ms_regression_view;
DROP TABLE IF EXISTS ms_alias_view;
DROP TABLE IF EXISTS ms_view_base;
DROP TABLE IF EXISTS ms_view_unaliased;
DROP TABLE IF EXISTS ms_shadow;
DROP TABLE IF EXISTS ms_outer;
DROP TABLE IF EXISTS ms_tup;
DROP TABLE IF EXISTS ms_wide;

CREATE TABLE ms_regression (k UInt32, v Int64) ENGINE = Memory;
INSERT INTO ms_regression VALUES (0, 10), (1, 20);
CREATE TABLE ms_regression_source (k UInt32, v Int64) ENGINE = Memory;
INSERT INTO ms_regression_source VALUES (0, 10), (1, 20);

-- Nested SETTINGS (including single-node INTERSECT trailing SETTINGS) must be applied before deciding whether to merge.
SELECT * FROM (
    SELECT
        (SELECT count() FROM ms_regression WHERE k > 10),
        (SELECT count() FROM ms_regression WHERE k < 2)
    SETTINGS empty_result_for_aggregation_by_empty_set = 1
);
SELECT * FROM (
    SELECT
        (SELECT count() FROM ms_regression WHERE k > 10),
        (SELECT count() FROM ms_regression WHERE k < 2)
    SETTINGS optimize_merge_scalar_subqueries = 0, empty_result_for_aggregation_by_empty_set = 1
);
SELECT * FROM (
    SELECT (SELECT count() FROM ms_regression WHERE k > 10), (SELECT count() FROM ms_regression WHERE k < 2)
    INTERSECT
    SELECT (SELECT count() FROM ms_regression WHERE k > 10), (SELECT count() FROM ms_regression WHERE k < 2)
    SETTINGS empty_result_for_aggregation_by_empty_set = 1
);

-- Aggregate aliases must not be silently removed from query analysis.

-- LIMIT AFTER and LIMIT UNTIL are part of the scalar subquery contract.
SELECT
    (SELECT count() FROM ms_regression WHERE k < 2 LIMIT AFTER count() > 10),
    (SELECT sum(v) FROM ms_regression WHERE k < 1);
SELECT
    (SELECT count() FROM ms_regression WHERE k < 2 LIMIT UNTIL count() > 0),
    (SELECT sum(v) FROM ms_regression WHERE k < 1);

-- A default expression must not be evaluated for rows rejected by its original subquery, even through `*`.
-- Without `PREWHERE` a single unmerged subquery already evaluates it for every row, so keep it enabled.
CREATE TABLE ms_regression_default (k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_regression_default VALUES (0), (1);
ALTER TABLE ms_regression_default ADD COLUMN v UInt32 DEFAULT intDiv(10, k);
SELECT
    (SELECT sum(v) FROM ms_regression_default WHERE k > 0),
    (SELECT count() FROM ms_regression_default WHERE k = 0)
SETTINGS optimize_move_to_prewhere = 1;
SELECT
    (SELECT count() FROM ms_regression_default WHERE k > 0 AND equals(*)),
    (SELECT count() FROM ms_regression_default WHERE k = 0)
SETTINGS optimize_move_to_prewhere = 1;

-- View, StorageAlias-to-View, and CTE expressions have the same evaluation boundary.
CREATE VIEW ms_regression_view AS SELECT k, intDiv(10, k) AS v FROM ms_regression;
CREATE TABLE ms_alias_view ENGINE = Alias(ms_regression_view);
SELECT
    (SELECT sum(v) FROM ms_regression_view WHERE k > 0),
    (SELECT count() FROM ms_regression_view WHERE k = 0);
SELECT
    (SELECT sum(v) FROM ms_alias_view WHERE k > 0),
    (SELECT count() FROM ms_alias_view WHERE k = 0);
WITH ms_regression AS (SELECT k, intDiv(10, k) AS v FROM ms_regression_source)
SELECT
    (SELECT sum(v) FROM ms_regression WHERE k > 0),
    (SELECT count() FROM ms_regression WHERE k = 0);

-- CREATE VIEW column metadata must retain standard `_subquery_N` names rather than internal rewrite artifacts.
CREATE TABLE ms_view_base (k UInt32, a Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_view_base VALUES (0, 10), (1, 20);
CREATE VIEW ms_view_unaliased AS SELECT (SELECT count() FROM ms_view_base WHERE k < 1), (SELECT sum(a) FROM ms_view_base WHERE k < 2);
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'ms_view_unaliased' ORDER BY position;

-- Session temporary tables shadow persistent tables of the same name.
CREATE TABLE ms_shadow (k UInt32, y Int64, x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_shadow VALUES (0, 100, 0);
CREATE TABLE ms_outer (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_outer VALUES (0), (1);
CREATE TEMPORARY TABLE ms_shadow (k UInt32, y Int64);
INSERT INTO ms_shadow VALUES (0, 10), (1, 20);
SELECT x, (SELECT sum(y) FROM ms_shadow WHERE k = x), (SELECT count() FROM ms_shadow WHERE k = x) FROM ms_outer ORDER BY x;
DROP TEMPORARY TABLE ms_shadow;

-- Set operations (including `nullIn`) must retain a real subquery argument.
SELECT exists(SELECT count() FROM ms_regression WHERE k < 1),
    (SELECT count() FROM ms_regression WHERE k < 1);
SELECT 1 IN (SELECT count() FROM ms_regression WHERE k < 1),
    (SELECT count() FROM ms_regression WHERE k < 1);
CREATE TABLE ms_tup (k UInt32, tup Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_tup VALUES (0, (1, 2)), (1, (3, 4));

-- Wide variable-length payload columns with selective filters keep PREWHERE granule pruning instead of full-table I/O amplification.
CREATE TABLE ms_wide (flag_rare UInt8, flag_common UInt8, payload String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
INSERT INTO ms_wide SELECT number = 5, 1, repeat(toString(number), 200) FROM numbers(500000);
SELECT (SELECT max(payload) FROM ms_wide WHERE flag_rare = 1) != '', (SELECT count() FROM ms_wide WHERE flag_common = 1)
SETTINGS max_bytes_to_read = 50000000, optimize_move_to_prewhere = 1;

DROP TABLE ms_wide;
DROP TABLE ms_tup;
DROP TABLE ms_outer;
DROP TABLE ms_shadow;
DROP TABLE ms_view_unaliased;
DROP TABLE ms_view_base;
DROP TABLE ms_alias_view;
DROP TABLE ms_regression_view;
DROP TABLE ms_regression_default;
DROP TABLE ms_regression_source;
DROP TABLE ms_regression;

SQL

${CLICKHOUSE_CLIENT} --multiquery <<'SQL'
-- `additional_table_filters` are keyed by table alias and must not be dropped or shared by merging.
CREATE TABLE ms_filter (k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_filter VALUES (0), (1);
SELECT (SELECT count() FROM ms_filter AS a WHERE k < 2), (SELECT count() FROM ms_filter AS b WHERE k < 2)
SETTINGS additional_table_filters = {'a': 'k < 1', 'b': 'k >= 1'};

-- A mixed-scale decimal comparison can overflow on rows outside a subquery's own filter,
-- so such subqueries must not be merged.
CREATE TABLE ms_decimal (k UInt8, price Decimal(9, 2), thr Decimal(9, 4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_decimal VALUES (1, 10.00, 10.0000);
INSERT INTO ms_decimal VALUES (2, 300000.00, 1.0000);
SELECT (SELECT count() FROM ms_decimal WHERE k = 1 AND price = thr), (SELECT count() FROM ms_decimal WHERE k = 2);

DROP TABLE ms_filter;
DROP TABLE ms_decimal;
SQL

# The two expected-failure cases, checked explicitly instead of `.sql` error tags.
${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE ms_err_alias (k UInt32, v Int64) ENGINE = Memory;
INSERT INTO ms_err_alias VALUES (0, 10), (1, 20);
CREATE TABLE ms_err_tup (k UInt32, tup Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_err_tup VALUES (0, (1, 2)), (1, (3, 4));
"
${CLICKHOUSE_CLIENT} --query "SELECT (SELECT sum(v) AS k FROM ms_err_alias WHERE k < 1), (SELECT sum(v) FROM ms_err_alias WHERE k > 0)" 2>&1 | grep -o "ILLEGAL_AGGREGATION" | head -n1
${CLICKHOUSE_CLIENT} --query "SELECT nullIn(1, (SELECT min(tup) FROM ms_err_tup WHERE k = 0)), (SELECT count() FROM ms_err_tup WHERE k = 1)" 2>&1 | grep -o "TYPE_MISMATCH" | head -n1
${CLICKHOUSE_CLIENT} --multiquery --query "DROP TABLE ms_err_alias; DROP TABLE ms_err_tup;"

u_none="u_ms_none_${CLICKHOUSE_DATABASE}"
u_part="u_ms_part_${CLICKHOUSE_DATABASE}"
u_rp="u_ms_rp_${CLICKHOUSE_DATABASE}"
rp="rp_ms_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${u_none}, ${u_part}, ${u_rp}"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS ms_sec;
CREATE TABLE ms_sec (tenant UInt8, pub_col Int64, secret_col Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_sec VALUES (1, 10, 100), (2, 20, 200), (2, 30, 300);

CREATE USER ${u_none} NOT IDENTIFIED;
CREATE USER ${u_part} NOT IDENTIFIED;
CREATE USER ${u_rp} NOT IDENTIFIED;

GRANT SELECT(pub_col) ON ${CLICKHOUSE_DATABASE}.ms_sec TO ${u_part};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.ms_sec TO ${u_rp};

CREATE ROW POLICY ${rp}_restrict ON ${CLICKHOUSE_DATABASE}.ms_sec FOR SELECT USING tenant = 2 TO ${u_rp};
CREATE ROW POLICY ${rp}_all ON ${CLICKHOUSE_DATABASE}.ms_sec FOR SELECT USING 1 TO ALL EXCEPT ${u_rp};
"

echo "-- unprivileged user denied"
${CLICKHOUSE_CLIENT} --user "${u_none}" --query "
SELECT (SELECT count() FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE secret_col = 100),
       (SELECT count() FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE secret_col = 200)
" 2>&1 | grep -o "ACCESS_DENIED" | head -n1

echo "-- column-level grant: allowed columns merge and succeed"
${CLICKHOUSE_CLIENT} --user "${u_part}" --query "
SELECT (SELECT count() FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE pub_col = 10),
       (SELECT sum(pub_col) FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE pub_col >= 20)
"

echo "-- column-level grant: ungranted column in second subquery denied"
${CLICKHOUSE_CLIENT} --user "${u_part}" --query "
SELECT (SELECT count() FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE pub_col = 10),
       (SELECT sum(secret_col) FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE pub_col >= 20)
" 2>&1 | grep -o "ACCESS_DENIED" | head -n1

echo "-- row policy respected by merged scalar subqueries"
${CLICKHOUSE_CLIENT} --user "${u_rp}" --query "
SELECT (SELECT count() FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE pub_col >= 10),
       (SELECT sum(pub_col) FROM ${CLICKHOUSE_DATABASE}.ms_sec WHERE pub_col > 10)
"

${CLICKHOUSE_CLIENT} --query "
DROP ROW POLICY ${rp}_restrict ON ${CLICKHOUSE_DATABASE}.ms_sec;
DROP ROW POLICY ${rp}_all ON ${CLICKHOUSE_DATABASE}.ms_sec;
DROP TABLE ms_sec;
DROP USER ${u_none}, ${u_part}, ${u_rp};
"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t (k UInt32, a Int64, b Float64, n Nullable(Int64), c Nullable(UInt8), lc LowCardinality(UInt16), d Decimal(9, 2), bo Bool)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO t SELECT number, number % 100 - 50, number * 1.5, if(number % 3 = 0, NULL, number), if(number % 5 = 0, NULL, number % 2),
    number % 10, number / 7, number % 2 FROM numbers(10000);
CREATE TABLE m (k UInt32, a Int64, n Nullable(Int64)) ENGINE = Memory;
INSERT INTO m SELECT number, number % 7, if(number % 2 = 0, NULL, number) FROM numbers(1000);
"

i=0
while IFS= read -r query; do
    i=$((i + 1))
    merged=$(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --optimize_merge_scalar_subqueries 1 --log_comment "merged_$i" --query "$query" 2>&1)
    unmerged=$(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --optimize_merge_scalar_subqueries 0 --query "$query" 2>&1)
    echo "$merged"
    [ "$merged" == "$unmerged" ] || echo "Result differs from optimize_merge_scalar_subqueries = 0: $unmerged"
done <<'EOF'
SELECT (SELECT sum(a) FROM t WHERE c), (SELECT count() FROM t WHERE c = 0)
SELECT (SELECT sum(n) FROM t WHERE a > 100), (SELECT count() FROM t WHERE a < 0), toTypeName((SELECT min(n) FROM t WHERE a > 200))
SELECT (SELECT min(n) FROM t WHERE a > 100), (SELECT max(n) FROM t WHERE a < 0), (SELECT avg(n) FROM t WHERE a > 1000)
SELECT (SELECT min(lc) FROM t WHERE a > 10), toTypeName((SELECT max(lc) FROM t WHERE a < 0))
SELECT (SELECT avg(d) FROM t WHERE a > 10), (SELECT sum(d) FROM t WHERE a < 0), toTypeName((SELECT sum(d) FROM t WHERE a < 5))
SELECT (SELECT sum(bo) FROM t WHERE a > 10), (SELECT count() FROM t WHERE b < 100 OR c)
SELECT (SELECT sum(a) FROM t WHERE NOT c), (SELECT count() FROM t WHERE isNull(c)), (SELECT count() FROM t WHERE xor(c, 1))
SELECT (SELECT sum(a) FROM t WHERE a = 1 AND NOT bo), (SELECT count() FROM t WHERE a = 1 AND NOT bo), (SELECT sum(a) FROM t WHERE a = 1) SETTINGS aggregate_functions_null_for_empty = 1
SELECT (SELECT sumOrNull(a) FROM t WHERE a > 1000), (SELECT countOrNull() FROM t WHERE a > 1000), (SELECT SUM(a) FROM t WHERE a > 10)
SELECT (SELECT count(c) FROM t WHERE a > 0), (SELECT count(n) FROM t WHERE a < 0)
SELECT (SELECT sum(a) FROM t WHERE k), (SELECT count() FROM t WHERE b), (SELECT count() FROM t WHERE lc)
SELECT (SELECT count() FROM t WHERE 0), (SELECT sum(a) FROM t WHERE a < 3)
SELECT (SELECT count() FROM t WHERE a > 0) + (SELECT sum(a) FROM t WHERE a < 0)
SELECT (SELECT count() FROM m WHERE n > 5), (SELECT sum(a) FROM m WHERE n < 100), (SELECT avg(n) FROM m WHERE a = 3)
SELECT (SELECT count() FROM t WHERE b > 0), (SELECT sum(a) FROM t WHERE c > 0) SETTINGS max_columns_to_read = 2
EOF

# In only-analyze mode a table function argument is executed, but its `SELECT`-list siblings must stay unexecuted.
${CLICKHOUSE_CLIENT} --enable_analyzer 1 --log_comment "create_view" --query "
CREATE VIEW v AS SELECT (SELECT sum(a) FROM t WHERE b > 0) AS x FROM numbers(assumeNotNull((SELECT count() FROM t WHERE b > 1)) + 1)"
${CLICKHOUSE_CLIENT} --enable_analyzer 1 --log_comment "create_view_without_sibling" --query "
CREATE VIEW v2 AS SELECT 1 AS x FROM numbers(assumeNotNull((SELECT count() FROM t WHERE b > 1)) + 1)"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# Merged queries execute one scalar subquery and read each table once: 10000 rows of 't' or 1000 rows of 'm',
# plus the row of the outer query. The last query is not merged because of `max_columns_to_read`.
${CLICKHOUSE_CLIENT} --query "
SELECT log_comment, ProfileEvents['ScalarSubqueriesCacheMiss'], read_rows <= if(log_comment = 'merged_14', 1001, 10001)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'merged_%'
ORDER BY length(log_comment), log_comment"

# `CREATE VIEW` reads only what the table function argument reads, not the column 'a' of the unexecuted sibling.
${CLICKHOUSE_CLIENT} --query "
SELECT count(), uniqExact(read_bytes), min(read_bytes) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Create'
    AND log_comment IN ('create_view', 'create_view_without_sibling')"

# Merging `min`/`max` over key-only filters must not defeat the implicit `_minmax_count_projection`:
# such a group has no residual conditions, so the merged projection stays free of `-If`.
${CLICKHOUSE_CLIENT} --multiquery <<'SQL'
CREATE TABLE ms_minmax (p UInt8, k UInt32, a Int64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO ms_minmax SELECT number % 4, number, number FROM numbers(100000);
SELECT (SELECT min(k) FROM ms_minmax WHERE p = 1), (SELECT max(k) FROM ms_minmax WHERE p = 1)
SETTINGS log_comment = '05814_minmax_projection', optimize_use_implicit_projections = 1;
DROP TABLE ms_minmax;
SQL

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
SELECT read_rows < 1000, ProfileEvents['ScalarSubqueriesCacheMiss']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05814_minmax_projection'"
