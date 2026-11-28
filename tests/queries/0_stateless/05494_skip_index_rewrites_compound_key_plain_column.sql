-- Tags: no-parallel-replicas
-- A compound key mixes an expression rewritten by the analyzer (`multiIf` to `if` by `optimize_multiif_to_if`)
-- and a plain column. The plain column is a column of the table by definition and must not be taken
-- for a name collision of the rewritten form with another column: the primary key, a compound `minmax`
-- skip index and the partition key must all be used.

SET explain_query_plan_default = 'legacy';
SET optimize_multiif_to_if = 1, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_plain_pk;
CREATE TABLE t_plain_pk (v Int32, id UInt32)
ENGINE = MergeTree ORDER BY (multiIf(v > 0, v, 0), id)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_plain_pk SELECT number, number FROM numbers(100);

SELECT 'primary key';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_plain_pk WHERE multiIf(v > 0, v, 0) > 97) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_plain_pk WHERE multiIf(v > 0, v, 0) > 97;
DROP TABLE t_plain_pk;

DROP TABLE IF EXISTS t_plain_idx;
CREATE TABLE t_plain_idx (v Int32, id UInt32, INDEX idx (multiIf(v > 0, v, 0), id) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_plain_idx SELECT number, number FROM numbers(100);

SELECT 'skip index';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_plain_idx WHERE multiIf(v > 0, v, 0) > 97) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM t_plain_idx WHERE multiIf(v > 0, v, 0) > 97;
DROP TABLE t_plain_idx;

DROP TABLE IF EXISTS t_plain_part;
CREATE TABLE t_plain_part (tenant UInt32, v Int32)
ENGINE = MergeTree PARTITION BY (tenant, multiIf(v > 49, 1, 0)) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_plain_part SELECT 1, number FROM numbers(100);

SELECT 'partition key';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT v FROM t_plain_part WHERE multiIf(v > 49, 1, 0) = 1) WHERE explain LIKE '%Parts:%/%';
SELECT count() FROM t_plain_part WHERE multiIf(v > 49, 1, 0) = 1;
DROP TABLE t_plain_part;
