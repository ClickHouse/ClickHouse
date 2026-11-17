-- A merge reconciles a `Nested` subcolumn recomputed from its `DEFAULT` with the shared offsets the
-- source part stores. The defaults computed from that subcolumn must see the reconciled value too,
-- with both the horizontal and the vertical merge algorithm.
-- https://github.com/ClickHouse/ClickHouse/issues/86123

DROP TABLE IF EXISTS t_nested_default_dependents;
CREATE TABLE t_nested_default_dependents (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, enable_vertical_merge_algorithm = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_default_dependents VALUES (1, [10, 20]);
ALTER TABLE t_nested_default_dependents ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_default_dependents ADD COLUMN b_length UInt64 MATERIALIZED length(`n.b`);
ALTER TABLE t_nested_default_dependents ADD COLUMN `n.c` Array(String) DEFAULT arrayMap(x -> concat(x, 'z'), `n.b`);
INSERT INTO t_nested_default_dependents (id, `n.a`, `n.b`) VALUES (2, [30], ['y']);
OPTIMIZE TABLE t_nested_default_dependents FINAL;
SELECT 'horizontal', id, `n.b`, `n.c`, b_length, b_length = length(`n.b`) FROM t_nested_default_dependents ORDER BY id;
DROP TABLE t_nested_default_dependents;

DROP TABLE IF EXISTS t_nested_default_dependents;
CREATE TABLE t_nested_default_dependents (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_default_dependents VALUES (1, [10, 20]);
ALTER TABLE t_nested_default_dependents ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_default_dependents ADD COLUMN b_length UInt64 MATERIALIZED length(`n.b`);
ALTER TABLE t_nested_default_dependents ADD COLUMN `n.c` Array(String) DEFAULT arrayMap(x -> concat(x, 'z'), `n.b`);
INSERT INTO t_nested_default_dependents (id, `n.a`, `n.b`) VALUES (2, [30], ['y']);
OPTIMIZE TABLE t_nested_default_dependents FINAL;
SELECT 'vertical', id, `n.b`, `n.c`, b_length, b_length = length(`n.b`) FROM t_nested_default_dependents ORDER BY id;
DROP TABLE t_nested_default_dependents;

-- A column absent from all source parts must not prevent a merge of a table with `TTL GROUP BY`, whose
-- grouping keys may be computed and whose `SET` expressions read aggregate results.
DROP TABLE IF EXISTS t_ttl_group_by_absent_column;
CREATE TABLE t_ttl_group_by_absent_column (id UInt32, ts DateTime, v UInt64)
ENGINE = MergeTree ORDER BY (id, toStartOfDay(ts))
TTL ts GROUP BY id, toStartOfDay(ts) SET v = sum(v), ts = min(ts)
SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_ttl_group_by_absent_column VALUES (1, '2000-01-01 01:00:00', 10);
INSERT INTO t_ttl_group_by_absent_column VALUES (1, '2000-01-01 02:00:00', 20);
ALTER TABLE t_ttl_group_by_absent_column ADD COLUMN missing UInt64;
OPTIMIZE TABLE t_ttl_group_by_absent_column FINAL;
SELECT 'ttl group by', id, ts, v, missing FROM t_ttl_group_by_absent_column;
DROP TABLE t_ttl_group_by_absent_column;

-- The same with a `Nested` subcolumn whose `DEFAULT` reads the absent column.
DROP TABLE IF EXISTS t_ttl_group_by_nested_default;
CREATE TABLE t_ttl_group_by_nested_default (id UInt32, ts DateTime, `n.a` Array(UInt64))
ENGINE = MergeTree ORDER BY (id, toStartOfDay(ts))
TTL ts GROUP BY id, toStartOfDay(ts) SET ts = min(ts), `n.a` = any(`n.a`)
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_ttl_group_by_nested_default VALUES (1, '2000-01-01 01:00:00', [1, 2]);
INSERT INTO t_ttl_group_by_nested_default VALUES (1, '2000-01-01 02:00:00', [1, 2]);
ALTER TABLE t_ttl_group_by_nested_default ADD COLUMN m UInt64;
ALTER TABLE t_ttl_group_by_nested_default ADD COLUMN `n.b` Array(UInt64) DEFAULT arrayMap(x -> x + m, `n.a`);
OPTIMIZE TABLE t_ttl_group_by_nested_default FINAL;
SELECT 'ttl group by nested', id, ts, `n.a`, `n.b`, length(`n.a`) = length(`n.b`) FROM t_ttl_group_by_nested_default;
CHECK TABLE t_ttl_group_by_nested_default SETTINGS check_query_single_value_result = 1;
DROP TABLE t_ttl_group_by_nested_default;
