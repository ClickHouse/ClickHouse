-- Tags: no-fasttest
-- no-fasttest: Parquet

-- `FixedString(N)` compares with a `String` as if zero-padded, so `t = 'V0\0\0'` is true for a `FixedString(3)` value 'V0'.
-- Index analysis must agree. Each row prints the full-scan count, then the count of the same predicate in `WHERE`.

-- The exact-count projection answers `count() ... WHERE` from the index ranges, so a wrong range shows as a wrong count.
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_minmax;
DROP TABLE IF EXISTS t_expr;

CREATE TABLE src (k UInt8, t FixedString(3)) ENGINE = Memory;
INSERT INTO src VALUES (1, 'V0'), (1, 'X'), (1, 'A'), (1, 'V0\0'), (1, 'V1');

CREATE TABLE t_pk (k UInt8, t FixedString(3)) ENGINE = MergeTree ORDER BY (k, t) SETTINGS index_granularity = 1;
CREATE TABLE t_nullable (k UInt8, t Nullable(FixedString(3))) ENGINE = MergeTree ORDER BY (k, t) SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE t_lc (k UInt8, t LowCardinality(FixedString(3))) ENGINE = MergeTree ORDER BY (k, t) SETTINGS index_granularity = 1;
CREATE TABLE t_partition (k UInt8, t FixedString(3)) ENGINE = MergeTree PARTITION BY t ORDER BY k;
CREATE TABLE t_minmax (k UInt8, t FixedString(3), INDEX i t TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE t_expr (k UInt8, s String) ENGINE = MergeTree ORDER BY (k, toFixedString(s, 3)) SETTINGS index_granularity = 1;

INSERT INTO t_pk SELECT * FROM src;
INSERT INTO t_nullable SELECT * FROM src;
INSERT INTO t_lc SELECT * FROM src;
INSERT INTO t_partition SELECT * FROM src;
INSERT INTO t_minmax SELECT * FROM src;
INSERT INTO t_expr SELECT k, t FROM src;

SELECT 'pk =', (SELECT countIf(t = 'V0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t = 'V0\0\0');
SELECT 'pk = reversed', (SELECT countIf('V0\0\0' = t) FROM t_pk), (SELECT count() FROM t_pk WHERE 'V0\0\0' = t);
SELECT 'pk <=>', (SELECT countIf(t <=> 'V0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t <=> 'V0\0\0');
SELECT 'pk !=', (SELECT countIf(t != 'V0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t != 'V0\0\0');
SELECT 'pk <', (SELECT countIf(t < 'V0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t < 'V0\0\0');
SELECT 'pk <=', (SELECT countIf(t <= 'V0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t <= 'V0\0\0');
SELECT 'pk >', (SELECT countIf(t > 'V0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t > 'V0\0\0');
SELECT 'pk >=', (SELECT countIf(t >= 'V0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t >= 'V0\0\0');
SELECT 'pk BETWEEN', (SELECT countIf(t BETWEEN 'V0\0\0' AND 'V0\0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t BETWEEN 'V0\0\0' AND 'V0\0\0\0');
SELECT 'pk = 5 bytes', (SELECT countIf(t = 'V0\0\0\0\0') FROM t_pk), (SELECT count() FROM t_pk WHERE t = 'V0\0\0\0\0');

SELECT 'nullable =', (SELECT countIf(t = 'V0\0\0') FROM t_nullable), (SELECT count() FROM t_nullable WHERE t = 'V0\0\0');
SELECT 'nullable <', (SELECT countIf(t < 'V0\0\0') FROM t_nullable), (SELECT count() FROM t_nullable WHERE t < 'V0\0\0');
SELECT 'nullable >=', (SELECT countIf(t >= 'V0\0\0') FROM t_nullable), (SELECT count() FROM t_nullable WHERE t >= 'V0\0\0');
SELECT 'nullable !=', (SELECT countIf(t != 'V0\0\0') FROM t_nullable), (SELECT count() FROM t_nullable WHERE t != 'V0\0\0');
SELECT 'lc =', (SELECT countIf(t = 'V0\0\0') FROM t_lc), (SELECT count() FROM t_lc WHERE t = 'V0\0\0');
SELECT 'lc <', (SELECT countIf(t < 'V0\0\0') FROM t_lc), (SELECT count() FROM t_lc WHERE t < 'V0\0\0');
SELECT 'lc >=', (SELECT countIf(t >= 'V0\0\0') FROM t_lc), (SELECT count() FROM t_lc WHERE t >= 'V0\0\0');
SELECT 'lc !=', (SELECT countIf(t != 'V0\0\0') FROM t_lc), (SELECT count() FROM t_lc WHERE t != 'V0\0\0');
SELECT 'partition =', (SELECT countIf(t = 'V0\0\0') FROM t_partition), (SELECT count() FROM t_partition WHERE t = 'V0\0\0');
SELECT 'partition <', (SELECT countIf(t < 'V0\0\0') FROM t_partition), (SELECT count() FROM t_partition WHERE t < 'V0\0\0');
SELECT 'partition >=', (SELECT countIf(t >= 'V0\0\0') FROM t_partition), (SELECT count() FROM t_partition WHERE t >= 'V0\0\0');
SELECT 'partition !=', (SELECT countIf(t != 'V0\0\0') FROM t_partition), (SELECT count() FROM t_partition WHERE t != 'V0\0\0');
SELECT 'minmax =', (SELECT countIf(t = 'V0\0\0') FROM t_minmax), (SELECT count() FROM t_minmax WHERE t = 'V0\0\0');
SELECT 'minmax <', (SELECT countIf(t < 'V0\0\0') FROM t_minmax), (SELECT count() FROM t_minmax WHERE t < 'V0\0\0');
SELECT 'minmax >=', (SELECT countIf(t >= 'V0\0\0') FROM t_minmax), (SELECT count() FROM t_minmax WHERE t >= 'V0\0\0');
SELECT 'minmax !=', (SELECT countIf(t != 'V0\0\0') FROM t_minmax), (SELECT count() FROM t_minmax WHERE t != 'V0\0\0');
SELECT 'expr =', (SELECT countIf(toFixedString(s, 3) = 'V0\0\0') FROM t_expr), (SELECT count() FROM t_expr WHERE toFixedString(s, 3) = 'V0\0\0');
SELECT 'expr <', (SELECT countIf(toFixedString(s, 3) < 'V0\0\0') FROM t_expr), (SELECT count() FROM t_expr WHERE toFixedString(s, 3) < 'V0\0\0');
SELECT 'expr >=', (SELECT countIf(toFixedString(s, 3) >= 'V0\0\0') FROM t_expr), (SELECT count() FROM t_expr WHERE toFixedString(s, 3) >= 'V0\0\0');
SELECT 'expr !=', (SELECT countIf(toFixedString(s, 3) != 'V0\0\0') FROM t_expr), (SELECT count() FROM t_expr WHERE toFixedString(s, 3) != 'V0\0\0');

SELECT 'const Nullable', (SELECT countIf(t = toNullable('V0\0\0')) FROM t_pk), (SELECT count() FROM t_pk WHERE t = toNullable('V0\0\0'));
SELECT 'const LowCardinality', (SELECT countIf(t = CAST('V0\0\0' AS LowCardinality(String))) FROM t_pk), (SELECT count() FROM t_pk WHERE t = CAST('V0\0\0' AS LowCardinality(String)));
SELECT 'const Variant', (SELECT countIf(t = 'V0\0\0'::Variant(String, UInt64)) FROM t_pk), (SELECT count() FROM t_pk WHERE t = 'V0\0\0'::Variant(String, UInt64));
SELECT 'const Dynamic', (SELECT countIf(t = 'V0\0\0'::Dynamic) FROM t_pk), (SELECT count() FROM t_pk WHERE t = 'V0\0\0'::Dynamic);

-- These rows depend on how `<=>` with a `Nullable` operand treats the trailing zero bytes of `FixedString`, so they only check that the index agrees with the full scan.
SELECT 'nullable <=> 3 bytes', (SELECT countIf(t <=> 'V0\0') FROM t_nullable) = (SELECT count() FROM t_nullable WHERE t <=> 'V0\0');
SELECT 'nullable <=> 4 bytes', (SELECT countIf(t <=> 'V0\0\0') FROM t_nullable) = (SELECT count() FROM t_nullable WHERE t <=> 'V0\0\0');
SELECT 'nullable <=> 2 bytes', (SELECT countIf(t <=> 'V0') FROM t_nullable), (SELECT count() FROM t_nullable WHERE t <=> 'V0');
SELECT 'pk <=> Nullable', (SELECT countIf(t <=> toNullable('V0\0\0')) FROM t_pk) = (SELECT count() FROM t_pk WHERE t <=> toNullable('V0\0\0'));
SELECT 'lc <=>', (SELECT countIf(t <=> 'V0\0\0') FROM t_lc), (SELECT count() FROM t_lc WHERE t <=> 'V0\0\0');

SELECT 'pk = nonzero tail', (SELECT countIf(t = 'V0\0x') FROM t_pk), (SELECT count() FROM t_pk WHERE t = 'V0\0x');
SELECT 'pk < nonzero tail', (SELECT countIf(t < 'V0\0x') FROM t_pk), (SELECT count() FROM t_pk WHERE t < 'V0\0x');
SELECT 'pk > nonzero tail', (SELECT countIf(t > 'V0\0x') FROM t_pk), (SELECT count() FROM t_pk WHERE t > 'V0\0x');

-- The primary key still prunes: the zero-tail constant selects the same granules as 'V0', also for a range.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k, t FROM t_pk WHERE t = 'V0' SETTINGS enable_parallel_replicas = 0, use_skip_indexes = 0, use_query_condition_cache = 0) WHERE explain ILIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k, t FROM t_pk WHERE t = 'V0\0\0' SETTINGS enable_parallel_replicas = 0, use_skip_indexes = 0, use_query_condition_cache = 0) WHERE explain ILIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k, t FROM t_pk WHERE t < 'V0' SETTINGS enable_parallel_replicas = 0, use_skip_indexes = 0, use_query_condition_cache = 0) WHERE explain ILIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k, t FROM t_pk WHERE t < 'V0\0\0' SETTINGS enable_parallel_replicas = 0, use_skip_indexes = 0, use_query_condition_cache = 0) WHERE explain ILIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k, t FROM t_nullable WHERE t <=> 'V0' SETTINGS enable_parallel_replicas = 0, use_skip_indexes = 0, use_query_condition_cache = 0) WHERE explain ILIKE '%Granules:%';

INSERT INTO FUNCTION file(currentDatabase() || '_05556.parquet', Parquet) SELECT t FROM src SETTINGS output_format_parquet_row_group_size = 1, engine_file_truncate_on_insert = 1;
SELECT 'parquet =', (SELECT countIf(t = 'V0\0\0') FROM file(currentDatabase() || '_05556.parquet', Parquet, 't FixedString(3)')), (SELECT count() FROM file(currentDatabase() || '_05556.parquet', Parquet, 't FixedString(3)') WHERE t = 'V0\0\0');
SELECT 'parquet >=', (SELECT countIf(t >= 'V0\0\0') FROM file(currentDatabase() || '_05556.parquet', Parquet, 't FixedString(3)')), (SELECT count() FROM file(currentDatabase() || '_05556.parquet', Parquet, 't FixedString(3)') WHERE t >= 'V0\0\0');

DROP TABLE src;
DROP TABLE t_pk;
DROP TABLE t_nullable;
DROP TABLE t_lc;
DROP TABLE t_partition;
DROP TABLE t_minmax;
DROP TABLE t_expr;
