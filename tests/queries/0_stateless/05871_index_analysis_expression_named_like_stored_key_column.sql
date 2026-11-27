-- Tags: no-replicated-database
-- A key or index that reads a stored column named like an expression must not prune a filter that computes that expression.
-- https://github.com/ClickHouse/ClickHouse/issues/123151

DROP TABLE IF EXISTS t_pk;
CREATE TABLE t_pk (s String, `lower(s)` String) ENGINE = MergeTree ORDER BY `lower(s)` SETTINGS index_granularity = 1;
INSERT INTO t_pk VALUES ('HELLO', 'zzz');
SELECT count() FROM t_pk WHERE lower(s) = 'hello';
SELECT s FROM t_pk WHERE lower(s) = 'hello';
SELECT count() FROM t_pk WHERE `lower(s)` = 'zzz';
DROP TABLE t_pk;

-- The key still prunes by the stored column.
DROP TABLE IF EXISTS t_pk_prune;
CREATE TABLE t_pk_prune (s String, `lower(s)` String) ENGINE = MergeTree ORDER BY `lower(s)` SETTINGS index_granularity = 1;
INSERT INTO t_pk_prune SELECT toString(number), toString(number) FROM numbers(100);
OPTIMIZE TABLE t_pk_prune FINAL;
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_pk_prune WHERE `lower(s)` = '42' SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0)
WHERE explain ILIKE '%Granules: 2/100%';
DROP TABLE t_pk_prune;

DROP TABLE IF EXISTS t_part;
CREATE TABLE t_part (s String, `lower(s)` String) ENGINE = MergeTree PARTITION BY `lower(s)` ORDER BY tuple();
INSERT INTO t_part VALUES ('HELLO', 'zzz');
SELECT count() FROM t_part WHERE lower(s) = 'hello';
SELECT s FROM t_part WHERE lower(s) = 'hello';
DROP TABLE t_part;

DROP TABLE IF EXISTS t_minmax;
CREATE TABLE t_minmax (id UInt64, `plus(id, 1)` UInt64, INDEX i `plus(id, 1)` TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_minmax VALUES (4, 100);
SELECT count() FROM t_minmax WHERE id + 1 = 5;
SELECT count() FROM t_minmax WHERE `plus(id, 1)` = 100 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';
DROP TABLE t_minmax;

DROP TABLE IF EXISTS t_set;
CREATE TABLE t_set (id UInt64, `plus(id, 1)` UInt64, INDEX i `plus(id, 1)` TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_set VALUES (4, 100);
SELECT count() FROM t_set WHERE id + 1 = 5 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_set WHERE id + 1 = 5 SETTINGS secondary_indices_enable_bulk_filtering = 1;
SELECT count() FROM t_set WHERE `plus(id, 1)` = 100 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';
DROP TABLE t_set;

DROP TABLE IF EXISTS t_set_tuple;
CREATE TABLE t_set_tuple (id UInt64, `plus(id, 1)` UInt64, INDEX i (id, `plus(id, 1)`) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_set_tuple VALUES (4, 100);
SELECT count() FROM t_set_tuple WHERE id + 1 = 5 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_set_tuple WHERE id + 1 = 5 SETTINGS secondary_indices_enable_bulk_filtering = 1;
DROP TABLE t_set_tuple;

DROP TABLE IF EXISTS t_set_func;
CREATE TABLE t_set_func (s String, `lower(s)` String, INDEX i length(`lower(s)`) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_set_func VALUES ('HELLO', 'z');
SELECT count() FROM t_set_func WHERE length(lower(s)) = 5 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_set_func WHERE length(lower(s)) = 5 SETTINGS secondary_indices_enable_bulk_filtering = 1;
SELECT count() FROM t_set_func WHERE length(`lower(s)`) = 1 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';
DROP TABLE t_set_func;

-- The other way round: a stored column named like the expression of the index.
DROP TABLE IF EXISTS t_set_inverse;
CREATE TABLE t_set_inverse (s String, INDEX i lower(s) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_set_inverse VALUES ('HELLO');
ALTER TABLE t_set_inverse ADD COLUMN `lower(s)` String DEFAULT 'zzz' SETTINGS allow_suspicious_indices = 1;
SELECT count() FROM t_set_inverse WHERE `lower(s)` = 'zzz' SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_set_inverse WHERE `lower(s)` = 'zzz' SETTINGS secondary_indices_enable_bulk_filtering = 1;
SELECT count() FROM t_set_inverse WHERE lower(s) = 'hello' SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';
DROP TABLE t_set_inverse;

DROP TABLE IF EXISTS t_proj;
CREATE TABLE t_proj (id UInt64, s String, `lower(s)` String, PROJECTION p (SELECT * ORDER BY `lower(s)`))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_proj VALUES (1, 'HELLO', 'zzz'), (2, 'X', 'aaa');
SELECT count() FROM t_proj WHERE lower(s) = 'hello' SETTINGS optimize_use_projections = 1;
SELECT count() FROM t_proj WHERE `lower(s)` = 'aaa' SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p';
DROP TABLE t_proj;

-- Both key elements read the column `lower(s)`, also inside `length(lower(s))`.
DROP TABLE IF EXISTS t_both;
CREATE TABLE t_both (s String, `lower(s)` String) ENGINE = MergeTree ORDER BY (length(lower(s)), `lower(s)`) SETTINGS index_granularity = 1;
INSERT INTO t_both VALUES ('HELLO', 'zzz');
SELECT count() FROM t_both WHERE lower(s) = 'hello';
SELECT count() FROM t_both WHERE length(lower(s)) = 5;
DROP TABLE t_both;

DROP TABLE IF EXISTS t_legacy;
CREATE TABLE t_legacy (x UInt64, `moduloLegacy(x, 16)` UInt64) ENGINE = MergeTree ORDER BY `moduloLegacy(x, 16)` SETTINGS index_granularity = 1;
INSERT INTO t_legacy VALUES (3, 99);
SELECT count() FROM t_legacy WHERE x % 16 = 3;
DROP TABLE t_legacy;

DROP TABLE IF EXISTS t_aj;
CREATE TABLE t_aj (arr Array(UInt8), `arrayJoin(arr)` UInt8) ENGINE = MergeTree ORDER BY `arrayJoin(arr)` SETTINGS index_granularity = 1;
INSERT INTO t_aj VALUES ([1, 2], 9);
SELECT count() FROM t_aj WHERE arrayJoin(arr) = 1;
DROP TABLE t_aj;

-- The body of a lambda is not a child of its node.
DROP TABLE IF EXISTS t_lambda_part;
CREATE TABLE t_lambda_part (arr Array(UInt64), `plus(x, 1)` UInt64)
ENGINE = MergeTree PARTITION BY arraySum(arrayMap(x -> `plus(x, 1)`, arr)) ORDER BY tuple();
INSERT INTO t_lambda_part VALUES ([1], 99);
SELECT count() FROM t_lambda_part WHERE arraySum(arrayMap(x -> x + 1, arr)) = 2;
ALTER TABLE t_lambda_part DELETE WHERE arraySum(arrayMap(x -> x + 1, arr)) = 2 SETTINGS mutations_sync = 2;
SELECT count() FROM t_lambda_part;
DROP TABLE t_lambda_part;

DROP TABLE IF EXISTS t_lambda_pk;
CREATE TABLE t_lambda_pk (arr Array(UInt64), y UInt64, `plus(x, y)` UInt64)
ENGINE = MergeTree ORDER BY arraySum(arrayMap(x -> `plus(x, y)`, arr)) SETTINGS index_granularity = 1;
INSERT INTO t_lambda_pk VALUES ([1], 1, 99);
SELECT count() FROM t_lambda_pk WHERE arraySum(arrayMap(x -> x + y, arr)) = 2;
DROP TABLE t_lambda_pk;

DROP TABLE IF EXISTS t_lambda_idx;
CREATE TABLE t_lambda_idx (arr Array(UInt64), `plus(x, 1)` UInt64,
    INDEX i_minmax arraySum(arrayMap(x -> `plus(x, 1)`, arr)) TYPE minmax GRANULARITY 1,
    INDEX i_set arraySum(arrayMap(x -> `plus(x, 1)`, arr)) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_lambda_idx VALUES ([1], 99);
SELECT count() FROM t_lambda_idx WHERE arraySum(arrayMap(x -> x + 1, arr)) = 2 SETTINGS ignore_data_skipping_indices = 'i_set';
SELECT count() FROM t_lambda_idx WHERE arraySum(arrayMap(x -> x + 1, arr)) = 2 SETTINGS ignore_data_skipping_indices = 'i_minmax';
DROP TABLE t_lambda_idx;

DROP TABLE IF EXISTS t_lambda_inverse;
CREATE TABLE t_lambda_inverse (arr Array(UInt64), `plus(x, 1)` UInt64,
    INDEX i arraySum(arrayMap(x -> x + 1, arr)) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY arraySum(arrayMap(x -> x + 1, arr)) SETTINGS index_granularity = 1;
INSERT INTO t_lambda_inverse VALUES ([1], 99);
SELECT count() FROM t_lambda_inverse WHERE arraySum(arrayMap(x -> `plus(x, 1)`, arr)) = 99 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_lambda_inverse WHERE arraySum(arrayMap(x -> `plus(x, 1)`, arr)) = 99 SETTINGS use_primary_key = 0, secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_lambda_inverse WHERE arraySum(arrayMap(x -> `plus(x, 1)`, arr)) = 99 SETTINGS use_primary_key = 0, secondary_indices_enable_bulk_filtering = 1;
SELECT count() FROM t_lambda_inverse WHERE arraySum(arrayMap(x -> x + 1, arr)) = 2 SETTINGS force_primary_key = 1, force_data_skipping_indices = 'i';
DROP TABLE t_lambda_inverse;

-- A stored column that the key does not read leaves the analysis unchanged.
DROP TABLE IF EXISTS t_unread;
CREATE TABLE t_unread (id UInt64, s String, `lower(s)` String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_unread SELECT number, 'X', 'y' FROM numbers(100);
OPTIMIZE TABLE t_unread FINAL;
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_unread WHERE (id, lower(s)) IN ((42, 'x')) SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0)
WHERE explain ILIKE '%Granules: 2/100%';
DROP TABLE t_unread;

-- A mutation selects the parts it rewrites by the same analysis.
DROP TABLE IF EXISTS t_mut;
CREATE TABLE t_mut (s String, `lower(s)` String) ENGINE = MergeTree PARTITION BY `lower(s)` ORDER BY tuple();
INSERT INTO t_mut VALUES ('HELLO', 'zzz');
ALTER TABLE t_mut DELETE WHERE lower(s) = 'hello' SETTINGS mutations_sync = 2;
SELECT count() FROM t_mut;
DROP TABLE t_mut;

DROP TABLE IF EXISTS t_mut_rmt;
CREATE TABLE t_mut_rmt (s String, `lower(s)` String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_rmt', '1') PARTITION BY `lower(s)` ORDER BY tuple();
INSERT INTO t_mut_rmt VALUES ('HELLO', 'zzz');
ALTER TABLE t_mut_rmt DELETE WHERE lower(s) = 'hello' SETTINGS mutations_sync = 2;
SELECT count() FROM t_mut_rmt;
DROP TABLE t_mut_rmt;
