-- JOIN runtime filters on `Nullable` integer keys: the result does not depend on the filter, and NULL probe rows never pass it.

SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET enable_join_runtime_filters_index_analysis = 0;
SET join_runtime_filter_exact_values_limit = 10000;
SET join_runtime_bloom_filter_bytes = 524288;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build;
DROP TABLE IF EXISTS build_null;
DROP TABLE IF EXISTS build_empty;

CREATE TABLE probe (k Nullable(Int64), v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE build (k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES build;
CREATE TABLE build_null (k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE build_empty (k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO probe SELECT if(number % 10 = 0, NULL, number % 1000), number FROM numbers(100000);
-- Two parts, so the build side can be read by several streams whose partial filters are merged.
INSERT INTO build SELECT if(number % 5 = 0, NULL, number * 31) FROM numbers(20);
INSERT INTO build SELECT if(number % 5 = 0, NULL, number * 31) FROM numbers(20, 20);
INSERT INTO build_null SELECT NULL FROM numbers(3);

SELECT 'inner', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k = b.k SETTINGS log_comment = 'inner', max_threads = 4;
SELECT 'inner', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'right', count(), sum(v) FROM probe AS p RIGHT JOIN build AS b ON p.k = b.k;
SELECT 'right', count(), sum(v) FROM probe AS p RIGHT JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'semi', count(), sum(v) FROM probe AS p LEFT SEMI JOIN build AS b ON p.k = b.k;
SELECT 'semi', count(), sum(v) FROM probe AS p LEFT SEMI JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build AS b ON p.k = b.k;
SELECT 'anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'null-safe', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k <=> b.k;
SELECT 'null-safe', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k <=> b.k SETTINGS enable_join_runtime_filters = 0;

SELECT 'only NULL keys inner', count(), sum(v) FROM probe AS p INNER JOIN build_null AS b ON p.k = b.k;
SELECT 'only NULL keys inner', count(), sum(v) FROM probe AS p INNER JOIN build_null AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'only NULL keys anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build_null AS b ON p.k = b.k;
SELECT 'only NULL keys anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build_null AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'empty inner', count(), sum(v) FROM probe AS p INNER JOIN build_empty AS b ON p.k = b.k;
SELECT 'empty inner', count(), sum(v) FROM probe AS p INNER JOIN build_empty AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'empty anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build_empty AS b ON p.k = b.k;
SELECT 'empty anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build_empty AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

SELECT 'UInt8 inner', count(), sum(v) FROM (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k, v FROM probe) AS p INNER JOIN (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = 'UInt8 inner';
SELECT 'UInt8 inner', count(), sum(v) FROM (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k, v FROM probe) AS p INNER JOIN (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'UInt8 anti', count(), sum(v) FROM (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k, v FROM probe) AS p LEFT ANTI JOIN (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k FROM build) AS b ON p.k = b.k;
SELECT 'UInt8 anti', count(), sum(v) FROM (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k, v FROM probe) AS p LEFT ANTI JOIN (SELECT CAST(k % 200, 'Nullable(UInt8)') AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'UInt128 inner', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(UInt128)') AS k, v FROM probe) AS p INNER JOIN (SELECT CAST(k, 'Nullable(UInt128)') AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = 'UInt128 inner';
SELECT 'UInt128 inner', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(UInt128)') AS k, v FROM probe) AS p INNER JOIN (SELECT CAST(k, 'Nullable(UInt128)') AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'UInt128 anti', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(UInt128)') AS k, v FROM probe) AS p LEFT ANTI JOIN (SELECT CAST(k, 'Nullable(UInt128)') AS k FROM build) AS b ON p.k = b.k;
SELECT 'UInt128 anti', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(UInt128)') AS k, v FROM probe) AS p LEFT ANTI JOIN (SELECT CAST(k, 'Nullable(UInt128)') AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'Date inner', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(Date)') AS k, v FROM probe) AS p INNER JOIN (SELECT CAST(k, 'Nullable(Date)') AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = 'Date inner';
SELECT 'Date inner', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(Date)') AS k, v FROM probe) AS p INNER JOIN (SELECT CAST(k, 'Nullable(Date)') AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'Date anti', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(Date)') AS k, v FROM probe) AS p LEFT ANTI JOIN (SELECT CAST(k, 'Nullable(Date)') AS k FROM build) AS b ON p.k = b.k;
SELECT 'Date anti', count(), sum(v) FROM (SELECT CAST(k, 'Nullable(Date)') AS k, v FROM probe) AS p LEFT ANTI JOIN (SELECT CAST(k, 'Nullable(Date)') AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

-- The build side has NULL keys and the probe side has NULL rows, but only rows with a non-NULL build key pass.
SYSTEM FLUSH LOGS query_log;
SELECT 'inner filter rows',
    ProfileEvents['RuntimeFilterRowsChecked'] > 0,
    ProfileEvents['RuntimeFilterRowsPassed'] = (SELECT count() FROM probe WHERE k IN (SELECT k FROM build WHERE k IS NOT NULL))
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'inner' AND type = 'QueryFinish';
SELECT 'UInt8 inner filter rows',
    ProfileEvents['RuntimeFilterRowsChecked'] > 0,
    ProfileEvents['RuntimeFilterRowsPassed'] = (SELECT count() FROM probe WHERE CAST(k % 200, 'Nullable(UInt8)') IN (SELECT CAST(k % 200, 'Nullable(UInt8)') FROM build WHERE k IS NOT NULL))
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'UInt8 inner' AND type = 'QueryFinish';
SELECT 'UInt128 inner filter rows',
    ProfileEvents['RuntimeFilterRowsChecked'] > 0,
    ProfileEvents['RuntimeFilterRowsPassed'] = (SELECT count() FROM probe WHERE CAST(k, 'Nullable(UInt128)') IN (SELECT CAST(k, 'Nullable(UInt128)') FROM build WHERE k IS NOT NULL))
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'UInt128 inner' AND type = 'QueryFinish';
SELECT 'Date inner filter rows',
    ProfileEvents['RuntimeFilterRowsChecked'] > 0,
    ProfileEvents['RuntimeFilterRowsPassed'] = (SELECT count() FROM probe WHERE CAST(k, 'Nullable(Date)') IN (SELECT CAST(k, 'Nullable(Date)') FROM build WHERE k IS NOT NULL))
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'Date inner' AND type = 'QueryFinish';

DROP TABLE probe;
DROP TABLE build;
DROP TABLE build_null;
DROP TABLE build_empty;
