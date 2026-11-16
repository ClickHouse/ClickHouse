-- with use_const_adaptive_granularity = 1 a merge sizes the granule of a wide part from the parts it merges, and a compact part by blocks
-- the twin table has the projection materialized and shows its marks and the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

SELECT '-- sum states of a merged wide part, the parts it merged can also give a compact part';
CREATE TABLE w_est (b UInt64, v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0,
        enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE w_real AS w_est;
ALTER TABLE w_real ADD PROJECTION p (SELECT b, sum(v) GROUP BY b);
-- 300 groups in each part, 100 of them in both
INSERT INTO w_est SELECT number, number FROM numbers(300);
INSERT INTO w_est SELECT number + 200, number + 300 FROM numbers(300);
INSERT INTO w_real SELECT number, number FROM numbers(300);
INSERT INTO w_real SELECT number + 200, number + 300 FROM numbers(300);
OPTIMIZE TABLE w_est FINAL;
OPTIMIZE TABLE w_real FINAL;

CREATE HYPOTHETICAL PROJECTION p ON w_est (SELECT b, sum(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM w_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real projection part: ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'w_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM w_real GROUP BY b);
DROP HYPOTHETICAL PROJECTION p ON w_est;

SELECT '-- groupArray states of the same width in a merged wide part, the states of the merged parts are not recorded';
CREATE TABLE g_est AS w_est;
CREATE TABLE g_real AS w_est;
ALTER TABLE g_real ADD PROJECTION p_a (SELECT b, groupArray(v) GROUP BY b);
INSERT INTO g_est SELECT number, number FROM numbers(300);
INSERT INTO g_est SELECT number, number FROM numbers(300);
INSERT INTO g_real SELECT number, number FROM numbers(300);
INSERT INTO g_real SELECT number, number FROM numbers(300);
OPTIMIZE TABLE g_est FINAL;
OPTIMIZE TABLE g_real FINAL;
CREATE HYPOTHETICAL PROJECTION p_a ON g_est (SELECT b, groupArray(v) GROUP BY b);
-- the high end of marks_span comes from the memory of the states, and a debug build uses more memory for a state
SELECT replaceRegexpOne(replaceRegexpAll(trim(explain), '\\s+', ' '), '^(marks_span: \\d+ to )\\d+$', '\\1N') AS line
FROM (EXPLAIN WHATIF SELECT b, groupArray(v) FROM g_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real projection part: ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'g_real' AND name = 'p_a' AND active;
DROP HYPOTHETICAL PROJECTION p_a ON g_est;

SELECT '-- groupArray and uniqExact states of a merged compact part';
CREATE TABLE c_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1,
        min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, merge_max_block_size = 8192,
        enable_vertical_merge_algorithm = 0, enable_block_number_column = 0, enable_block_offset_column = 0,
        ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE c_real AS c_est;
ALTER TABLE c_real ADD PROJECTION p (SELECT b, groupArray(v) GROUP BY b);
ALTER TABLE c_real ADD PROJECTION p_u (SELECT b, uniqExact(v) GROUP BY b);
INSERT INTO c_est SELECT number, number % 500, 0 FROM numbers(10000);
INSERT INTO c_est SELECT number, number % 500, 0 FROM numbers(10000);
INSERT INTO c_real SELECT number, number % 500, 0 FROM numbers(10000);
INSERT INTO c_real SELECT number, number % 500, 0 FROM numbers(10000);
OPTIMIZE TABLE c_est FINAL;
OPTIMIZE TABLE c_real FINAL;
SELECT 'part types: ' || toString(groupUniqArray(part_type)) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'c_real' AND active;

CREATE HYPOTHETICAL PROJECTION p ON c_est (SELECT b, groupArray(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, groupArray(v) FROM c_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real projection part: ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'c_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT b, groupArray(v) FROM c_real GROUP BY b);
DROP HYPOTHETICAL PROJECTION p ON c_est;

CREATE HYPOTHETICAL PROJECTION p_u ON c_est (SELECT b, uniqExact(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, uniqExact(v) FROM c_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real projection part: ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'c_real' AND name = 'p_u' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT b, uniqExact(v) FROM c_real GROUP BY b);

DROP TABLE w_est;
DROP TABLE w_real;
DROP TABLE g_est;
DROP TABLE g_real;
DROP TABLE c_est;
DROP TABLE c_real;
