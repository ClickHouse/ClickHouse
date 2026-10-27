-- a sample of granules cannot show how many groups a part has, so a sampled estimate of an aggregate projection
-- takes the groups of each part from the `uniq` statistics of its GROUP BY key
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0, allow_experimental_statistics = 1, materialize_statistics_on_insert = 1;

CREATE TABLE t_est (a UInt64, k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT k, sum(v) GROUP BY k);

-- 2000 groups in 600 granules: the sample sees almost every group
INSERT INTO t_est SELECT number, number % 2000, number FROM numbers(60000);
INSERT INTO t_real SELECT number, number % 2000, number FROM numbers(60000);
CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT k, sum(v) GROUP BY k);
SELECT '-- 2000 groups';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 6000 SELECT k, sum(v) FROM t_est WHERE k < 300 GROUP BY k)
WHERE match(line, '^(With |status:|marks:|rows:|marks_span|verdict:|empirical_status|empirical_reason|sampled_marks)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT k, sum(v) FROM t_real WHERE k < 300 GROUP BY k);

-- about 40000 groups in 600 granules: the sample sees a part of them
CREATE TABLE h_est AS t_est;
CREATE TABLE h_real AS t_real;
INSERT INTO h_est SELECT number, intHash64(number) % 40000, number FROM numbers(60000);
INSERT INTO h_real SELECT number, intHash64(number) % 40000, number FROM numbers(60000);
CREATE HYPOTHETICAL PROJECTION p ON h_est (SELECT k, sum(v) GROUP BY k);
SELECT '-- about 40000 groups';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 6000 SELECT k, sum(v) FROM h_est WHERE k < 4000 GROUP BY k)
WHERE match(line, '^(With |status:|marks:|rows:|marks_span|verdict:|empirical_status|empirical_reason|sampled_marks)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT k, sum(v) FROM h_real WHERE k < 4000 GROUP BY k);

-- without statistics of the key the sampled estimate is not supported
CREATE TABLE n_est (a UInt64, k UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        auto_statistics_types = '';
INSERT INTO n_est SELECT number, number % 2000, number FROM numbers(60000);
CREATE HYPOTHETICAL PROJECTION p ON n_est (SELECT k, sum(v) GROUP BY k);
SELECT '-- no statistics';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 6000 SELECT k, sum(v) FROM n_est WHERE k < 300 GROUP BY k)
WHERE match(line, '^(empirical_status|empirical_reason):');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE h_est;
DROP TABLE h_real;
DROP TABLE n_est;
