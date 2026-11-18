-- The statistics of a JOIN runtime filter count the probe rows, whatever form the key column is in. A `LowCardinality`,
-- sparse, constant or lazily replicated key is looked up by the values that it stores, each once, and counts the same
-- rows as the plain key with the same data. The statistics decide whether the filter is worth evaluating, see
-- `join_runtime_filter_pass_ratio_threshold_for_disabling`.

SET enable_analyzer = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_runtime_filter_exact_values_limit = 100;
SET join_algorithm = 'hash', query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET join_runtime_filter_from_fixed_hash_table = 0;
-- Every probe row is checked: the join reads the probe side once it has built its hash table, by when the filter is
-- built too, no block is skipped, and no granule is pruned.
SET join_runtime_filter_pass_ratio_threshold_for_disabling = 2, enable_join_runtime_filters_index_analysis = 0;

DROP TABLE IF EXISTS t_probe_string;
DROP TABLE IF EXISTS t_probe_low_cardinality;
DROP TABLE IF EXISTS t_probe_nullable;
DROP TABLE IF EXISTS t_probe_low_cardinality_nullable;
DROP TABLE IF EXISTS t_build_string;
DROP TABLE IF EXISTS t_build_low_cardinality;
DROP TABLE IF EXISTS t_probe_full;
DROP TABLE IF EXISTS t_probe_sparse;
DROP TABLE IF EXISTS t_build_uint64;
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_probe_tuple;
DROP TABLE IF EXISTS t_build_tuple;
DROP TABLE IF EXISTS t_probe_many_values;

CREATE TABLE t_probe_string (k String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_low_cardinality (k LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_nullable (k Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_low_cardinality_nullable (k LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_string (k String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_low_cardinality (k LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_full (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_probe_sparse (k UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;
CREATE TABLE t_build_uint64 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_left (id UInt64, k String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_right (id UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_tuple (k Tuple(a UInt64, b LowCardinality(String))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;
CREATE TABLE t_build_tuple (k Tuple(a UInt64, b LowCardinality(String))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_many_values (id UInt64, k LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();

-- A skewed key: 99% of the rows have one value, which passes.
INSERT INTO t_probe_string SELECT if(number % 100 = 0, 'miss', 'hit') FROM numbers(100000);
INSERT INTO t_probe_low_cardinality SELECT k FROM t_probe_string;
INSERT INTO t_probe_nullable SELECT multiIf(number % 100 = 0, 'miss', number % 100 = 1, NULL, 'hit') FROM numbers(100000);
INSERT INTO t_probe_low_cardinality_nullable SELECT k FROM t_probe_nullable;
INSERT INTO t_build_string VALUES ('hit'), ('other');
INSERT INTO t_build_low_cardinality VALUES ('hit'), ('other');
-- 99% of the rows have the default 0, which passes.
INSERT INTO t_probe_full SELECT if(number % 100 = 0, number + 1, 0) FROM numbers(100000);
INSERT INTO t_probe_sparse SELECT k FROM t_probe_full;
INSERT INTO t_build_uint64 VALUES (0), (7);
-- Each row of `t_left` joins 100 rows of `t_right`, so that join replicates its `k` lazily, which is the key of the next join.
INSERT INTO t_left SELECT number, if(number % 100 = 0, 'miss', 'hit') FROM numbers(1000);
INSERT INTO t_right SELECT number % 1000 FROM numbers(100000);
-- A sparse and a `LowCardinality` element inside the key: 98% of the rows have the tuple that passes.
INSERT INTO t_probe_tuple SELECT (if(number % 100 = 0, number + 1, 0), if(number % 100 = 1, 'miss', 'hit')) FROM numbers(100000);
INSERT INTO t_build_tuple VALUES ((0, 'hit')), ((7, 'other'));
-- The dictionary has 998 values. Every 1000th row is one of the 100 rows with the values '0', '3', ..., '297'.
INSERT INTO t_probe_many_values SELECT number, toString(number % 997) FROM numbers(100000);

SELECT count() FROM t_probe_string AS p INNER JOIN t_build_string AS b ON p.k = b.k
SETTINGS log_comment = 'key String';
SELECT count() FROM t_probe_low_cardinality AS p INNER JOIN t_build_low_cardinality AS b ON p.k = b.k
SETTINGS log_comment = 'key String LowCardinality';
SELECT count() FROM (SELECT 'hit' AS k FROM t_probe_string) AS p INNER JOIN t_build_string AS b ON p.k = b.k
SETTINGS log_comment = 'key String constant';
SELECT count() FROM t_probe_nullable AS p INNER JOIN t_build_string AS b ON p.k = b.k
SETTINGS log_comment = 'key Nullable(String)';
SELECT count() FROM t_probe_low_cardinality_nullable AS p INNER JOIN t_build_low_cardinality AS b ON p.k = b.k
SETTINGS log_comment = 'key Nullable(String) LowCardinality';
SELECT count() FROM t_probe_full AS p INNER JOIN t_build_uint64 AS b ON p.k = b.k
SETTINGS log_comment = 'key UInt64';
SELECT count() FROM t_probe_sparse AS p INNER JOIN t_build_uint64 AS b ON p.k = b.k
SETTINGS log_comment = 'key UInt64 sparse';
SELECT count() FROM (SELECT l.k AS k FROM t_left AS l FULL JOIN t_right AS r ON l.id = r.id) AS p INNER JOIN t_build_string AS b ON p.k = b.k
SETTINGS enable_lazy_columns_replication = 1, log_comment = 'key String replicated';
SELECT count() FROM t_probe_tuple AS p INNER JOIN t_build_tuple AS b ON p.k = b.k
SETTINGS log_comment = 'key Tuple of sparse and LowCardinality';
-- The filter of the subquery leaves a few rows of each block, and the dictionary as it was. The rows are the fewer to
-- look up then. `LIMIT` keeps the runtime filter above that filter.
SELECT count() FROM (SELECT k FROM t_probe_many_values WHERE id % 1000 = 0 LIMIT 100000) AS p
INNER JOIN (SELECT toLowCardinality(toString(number * 3)) AS k FROM numbers(2)) AS b ON p.k = b.k
SETTINGS log_comment = 'key String LowCardinality with fewer rows than values';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['RuntimeFilterRowsChecked'] AS checked,
    ProfileEvents['RuntimeFilterRowsPassed'] AS passed,
    ProfileEvents['RuntimeFilterRowsSkipped'] AS skipped
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment LIKE 'key %'
ORDER BY log_comment;

DROP TABLE t_probe_string;
DROP TABLE t_probe_low_cardinality;
DROP TABLE t_probe_nullable;
DROP TABLE t_probe_low_cardinality_nullable;
DROP TABLE t_build_string;
DROP TABLE t_build_low_cardinality;
DROP TABLE t_probe_full;
DROP TABLE t_probe_sparse;
DROP TABLE t_build_uint64;
DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE t_probe_tuple;
DROP TABLE t_build_tuple;
DROP TABLE t_probe_many_values;
