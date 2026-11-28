-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: the EXPLAIN assertion below requires the exact-count projection to
--   short-circuit on the initiator, which is suppressed when reading from remote replicas.
-- no-replicated-database: EXPLAIN output differs for a replicated database.

-- `count()` answered by the exact-count projection, under the lightweight primary key analysis, when a
-- negated set reads a `Nullable` key column that the in-memory primary index does not hold (it follows
-- a unique key prefix). Each count must equal the same query without the projection.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1, use_lightweight_primary_key_index_analysis = 1, transform_null_in = 0;

DROP TABLE IF EXISTS t_unloaded_nullable_key;
DROP TABLE IF EXISTS t_unloaded_nullable_key_empty;
DROP TABLE IF EXISTS t_unloaded_lc_nullable_key;
DROP TABLE IF EXISTS t_unloaded_nullable_key_partitioned;

CREATE TABLE t_unloaded_nullable_key (a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 1, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_unloaded_nullable_key SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(100);

CREATE TABLE t_unloaded_nullable_key_empty (b Nullable(UInt64)) ENGINE = Null;

SELECT 'NOT IN (NULL)';
SELECT count() FROM t_unloaded_nullable_key WHERE a >= 10 AND b NOT IN (NULL);
SELECT count() FROM t_unloaded_nullable_key WHERE a >= 10 AND b NOT IN (NULL) SETTINGS optimize_use_implicit_projections = 0;

SELECT 'NOT IN (empty subquery)';
SELECT count() FROM t_unloaded_nullable_key WHERE a >= 10 AND b NOT IN (SELECT b FROM t_unloaded_nullable_key_empty);
SELECT count() FROM t_unloaded_nullable_key WHERE a >= 10 AND b NOT IN (SELECT b FROM t_unloaded_nullable_key_empty) SETTINGS optimize_use_implicit_projections = 0;

SELECT 'OR on the prefix, NOT over IN';
SELECT count() FROM t_unloaded_nullable_key WHERE (a >= 10 OR a < 3) AND NOT (b IN (NULL));
SELECT count() FROM t_unloaded_nullable_key WHERE (a >= 10 OR a < 3) AND NOT (b IN (NULL)) SETTINGS optimize_use_implicit_projections = 0;

CREATE TABLE t_unloaded_lc_nullable_key (a UInt64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY (a, s)
SETTINGS index_granularity = 1, allow_nullable_key = 1, allow_suspicious_low_cardinality_types = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_unloaded_lc_nullable_key SELECT number, if(number % 5 = 0, NULL, toString(number)) FROM numbers(100);

SELECT 'LowCardinality(Nullable(String))';
SELECT count() FROM t_unloaded_lc_nullable_key WHERE a >= 10 AND s NOT IN (NULL);
SELECT count() FROM t_unloaded_lc_nullable_key WHERE a >= 10 AND s NOT IN (NULL) SETTINGS optimize_use_implicit_projections = 0;

-- `c` is bounded by the partition, so it is analysed, while `b` between `a` and `c` is not.
CREATE TABLE t_unloaded_nullable_key_partitioned (a UInt64, b Nullable(UInt64), c UInt8) ENGINE = MergeTree PARTITION BY c ORDER BY (a, b, c)
SETTINGS index_granularity = 1, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_unloaded_nullable_key_partitioned SELECT number, if(number % 7 = 0, NULL, number), 1 FROM numbers(100);

SELECT 'partition-bounded key column after the unloaded one';
SELECT count() FROM t_unloaded_nullable_key_partitioned WHERE c = 1 AND b NOT IN (NULL);
SELECT count() FROM t_unloaded_nullable_key_partitioned WHERE c = 1 AND b NOT IN (NULL) SETTINGS optimize_use_implicit_projections = 0;

SELECT 'a range condition on the unloaded column keeps the exact count';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_unloaded_nullable_key WHERE a >= 10 OR b < 5) WHERE explain ILIKE '%_exact_count_projection%';
SELECT count() FROM t_unloaded_nullable_key WHERE a >= 10 OR b < 5;
SELECT count() FROM t_unloaded_nullable_key WHERE a >= 10 OR b < 5 SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE t_unloaded_nullable_key;
DROP TABLE t_unloaded_nullable_key_empty;
DROP TABLE t_unloaded_lc_nullable_key;
DROP TABLE t_unloaded_nullable_key_partitioned;
