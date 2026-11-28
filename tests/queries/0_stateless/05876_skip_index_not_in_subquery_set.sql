-- A minmax or bloom_filter skip index cannot skip a granule by a `NOT IN` whose set is not built yet,
-- so it must not run the subquery while the query is planned. `EXPLAIN` plans the query without running it,
-- and the `throwIf` subquery fails as soon as anything runs it.
-- Parallel replicas run a `GLOBAL` subquery while planning, to send its set to the replicas: the `GLOBAL` oracles turn them off.

SET optimize_move_to_prewhere = 0, use_skip_indexes = 1, use_index_for_in_with_subqueries = 1;

DROP TABLE IF EXISTS t_mm;
DROP TABLE IF EXISTS t_bf;

CREATE TABLE t_mm (k UInt64, s String, INDEX i s TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128, index_granularity_bytes = 0;
INSERT INTO t_mm SELECT number, toString(intDiv(number, 128)) FROM numbers(1024);

CREATE TABLE t_bf (k UInt64, s String, INDEX i s TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128, index_granularity_bytes = 0;
INSERT INTO t_bf SELECT number, toString(intDiv(number, 128)) FROM numbers(1024);

SELECT 'minmax, NOT IN (subquery)';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_mm WHERE s NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1)));

SELECT 'minmax, GLOBAL NOT IN (subquery)';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_mm WHERE s GLOBAL NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1)) SETTINGS enable_parallel_replicas = 0);

SELECT 'minmax, NOT IN and GLOBAL NOT IN (subquery), transform_null_in = 1';
SET transform_null_in = 1;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_mm WHERE s NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1)));
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_mm WHERE s GLOBAL NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1)) SETTINGS enable_parallel_replicas = 0);
SET transform_null_in = 0;

SELECT 'bloom_filter, NOT IN and GLOBAL NOT IN (subquery), transform_null_in = 1';
SET transform_null_in = 1;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_bf WHERE s NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1)));
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_bf WHERE s GLOBAL NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1)) SETTINGS enable_parallel_replicas = 0);
SET transform_null_in = 0;

SELECT 'minmax, IN (subquery) still builds the set';
EXPLAIN SELECT count() FROM t_mm WHERE s IN (SELECT toString(throwIf(number = 0)) FROM numbers(1)) FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'minmax, NOT (NOT IN (subquery)) is an IN';
EXPLAIN SELECT count() FROM t_mm WHERE NOT (s NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1))) FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'minmax, NOT (GLOBAL NOT IN (subquery)), transform_null_in = 1, is an IN';
SET transform_null_in = 1;
EXPLAIN SELECT count() FROM t_mm WHERE NOT (s GLOBAL NOT IN (SELECT toString(throwIf(number = 0)) FROM numbers(1))) FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mm WHERE NOT (s GLOBAL NOT IN (SELECT toString(number) FROM numbers(3)))) WHERE explain ILIKE '%Granules:%';
SELECT count() FROM t_mm WHERE NOT (s GLOBAL NOT IN (SELECT toString(number) FROM numbers(3)));
SET transform_null_in = 0;
SELECT count() FROM t_mm WHERE s IN (SELECT toString(number) FROM numbers(3));

SELECT 'minmax, NOT IN (literal list) still skips';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mm WHERE s NOT IN ('0', '1', '2')) WHERE explain ILIKE '%Granules:%';

SELECT 'minmax, force_data_skipping_indices';
SELECT count() FROM t_mm WHERE s NOT IN (SELECT toString(number) FROM numbers(3)) SETTINGS force_data_skipping_indices = 'i'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_mm WHERE s NOT IN ('0', '1', '2') SETTINGS force_data_skipping_indices = 'i';

SELECT 'results';
SELECT count() FROM t_mm WHERE s NOT IN (SELECT toString(number) FROM numbers(3));
SELECT count() FROM t_mm WHERE s NOT IN (SELECT toString(number) FROM numbers(3)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf WHERE s NOT IN (SELECT toString(number) FROM numbers(3)) SETTINGS transform_null_in = 1;
SELECT count() FROM t_bf WHERE s NOT IN (SELECT toString(number) FROM numbers(3)) SETTINGS transform_null_in = 1, use_skip_indexes = 0;

DROP TABLE t_mm;
DROP TABLE t_bf;
