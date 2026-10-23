-- rows of different widths change the granules of a wide part only when a granule of the widest rows reaches `index_granularity_bytes`
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t_est (a UInt64, k String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT a, k ORDER BY k);
INSERT INTO t_est SELECT number, toString(number % 1000) FROM numbers(20000);
INSERT INTO t_real SELECT number, toString(number % 1000) FROM numbers(20000);
CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT a, k ORDER BY k);

SELECT '-- the byte limit does not cut a granule';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a FROM t_est WHERE k = '42')
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows' FROM (EXPLAIN ESTIMATE SELECT a FROM t_real WHERE k = '42');

-- the same rows, with a byte limit under the bytes of 100 rows
CREATE TABLE b_est (a UInt64, k String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
INSERT INTO b_est SELECT number, toString(number % 1000) FROM numbers(20000);
CREATE HYPOTHETICAL PROJECTION p ON b_est (SELECT a, k ORDER BY k);
SELECT '-- the byte limit cuts granules';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a FROM b_est WHERE k = '42')
WHERE match(line, '^(With |verdict:)');

-- a compact part can end a granule at the end of each block
CREATE TABLE c_est (a UInt64, k String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1Gi', min_rows_for_wide_part = 1000000000,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
INSERT INTO c_est SELECT number, toString(number % 1000) FROM numbers(20000);
CREATE HYPOTHETICAL PROJECTION p ON c_est (SELECT a, k ORDER BY k);
SELECT '-- a compact part';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a FROM c_est WHERE k = '42')
WHERE match(line, '^(With |verdict:)');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE b_est;
DROP TABLE c_est;
