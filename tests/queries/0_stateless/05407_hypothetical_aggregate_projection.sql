-- an aggregate projection part keeps one row for each group of its part, with the merged states
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT b, sum(v), count() GROUP BY b);

-- two parts with the same 2000 groups
SYSTEM STOP MERGES t_est;
SYSTEM STOP MERGES t_real;
INSERT INTO t_est SELECT number, number % 2000, number FROM numbers(10000);
INSERT INTO t_est SELECT number + 10000, number % 2000, number FROM numbers(10000);
INSERT INTO t_real SELECT number, number % 2000, number FROM numbers(10000);
INSERT INTO t_real SELECT number + 10000, number % 2000, number FROM numbers(10000);

CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT b, sum(v), count() GROUP BY b);

SELECT '-- two parts, each with its own groups';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM t_est WHERE b < 300 GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|rows:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM t_real WHERE b < 300 GROUP BY b);

SELECT '-- the merged part has the groups of both parts once';
SYSTEM START MERGES t_est;
SYSTEM START MERGES t_real;
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM t_est WHERE b < 300 GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|rows:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM t_real WHERE b < 300 GROUP BY b);

-- one mark in the base table and one in the projection
CREATE TABLE e_est AS t_est;
CREATE TABLE e_real AS t_est;
ALTER TABLE e_real ADD PROJECTION p_e (SELECT b, sum(v) GROUP BY b);
INSERT INTO e_est SELECT number, number % 10, number FROM numbers(100);
INSERT INTO e_real SELECT number, number % 10, number FROM numbers(100);
CREATE HYPOTHETICAL PROJECTION p_e ON e_est (SELECT b, sum(v) GROUP BY b);
SELECT '-- the optimizer also takes an aggregate projection at the marks of the base table';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM e_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|verdict:|reason:)');
SELECT if(explain LIKE '%ReadFromMergeTree (p_e)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT b, sum(v) FROM e_real GROUP BY b) WHERE explain LIKE '%ReadFromMergeTree%';

SELECT '-- DISTINCT on the keys of the projection';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT DISTINCT b FROM t_est WHERE b < 300)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT DISTINCT b FROM t_real WHERE b < 300);

SELECT '-- the projection does not apply';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, sum(v) FROM t_est WHERE b < 300 GROUP BY a)
WHERE match(line, '^(status:|reason:)');
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, v FROM t_est WHERE b < 300)
WHERE match(line, '^(status:|reason:)');
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 3000 SELECT b, sum(v) FROM t_est WHERE b < 300 GROUP BY b)
WHERE match(line, '^(empirical_status:|empirical_reason:)');

-- a state of uniqExact grows with its count of values, so the rows have different widths and the verdict is not firm
CREATE TABLE u_est AS t_est;
INSERT INTO u_est SELECT number, number % 300, if(number % 300 < 150, number, 0) FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_u ON u_est (SELECT b, uniqExact(v) GROUP BY b);
SELECT '-- states of different sizes';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, uniqExact(v) FROM u_est WHERE b < 100 GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
DROP HYPOTHETICAL PROJECTION p ON t_est;
CREATE HYPOTHETICAL PROJECTION p_total ON t_est (SELECT sum(v));
SELECT '-- no GROUP BY keys';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT sum(v) FROM t_est)
WHERE match(line, '^(With |status:|reason:)');

-- a projection with WHERE aggregates only the rows that pass its filter
CREATE TABLE w_est AS t_est;
CREATE TABLE w_real AS t_est;
ALTER TABLE w_real ADD PROJECTION p_w (SELECT b, sum(v) WHERE v % 2 = 0 GROUP BY b);
INSERT INTO w_est SELECT number, number % 2000, number FROM numbers(10000);
INSERT INTO w_real SELECT number, number % 2000, number FROM numbers(10000);
CREATE HYPOTHETICAL PROJECTION p_w ON w_est (SELECT b, sum(v) WHERE v % 2 = 0 GROUP BY b);
SELECT '-- a projection with WHERE';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM w_est WHERE v % 2 = 0 AND b < 300 GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|rows:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT b, sum(v) FROM w_real WHERE v % 2 = 0 AND b < 300 GROUP BY b);

-- the exact count of whole ranges takes parts out of the base read, but the optimizer weighs the projection on all of them
CREATE TABLE c_est (d Date, id UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY (d, id)
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE c_real AS c_est;
ALTER TABLE c_real ADD PROJECTION p_c (SELECT d, count() GROUP BY d);
INSERT INTO c_est SELECT toDate('2026-01-01') + intDiv(number, 100), number FROM numbers(9000);
INSERT INTO c_real SELECT toDate('2026-01-01') + intDiv(number, 100), number FROM numbers(9000);
CREATE HYPOTHETICAL PROJECTION p_c ON c_est (SELECT d, count() GROUP BY d);
SELECT '-- an exact count of whole ranges';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT count() FROM c_est WHERE d >= '2026-01-15' AND d < '2026-03-10' SETTINGS optimize_use_implicit_projections = 1)
WHERE match(line, '^(Baseline|With |marks:|rows:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT count() FROM c_real WHERE d >= '2026-01-15' AND d < '2026-03-10' SETTINGS optimize_use_implicit_projections = 1);

-- the key of an aggregate projection can be an expression over the columns of the table
CREATE TABLE h_est (ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE h_real AS h_est;
ALTER TABLE h_real ADD PROJECTION p_h (SELECT toStartOfHour(ts), sum(v) GROUP BY toStartOfHour(ts));
INSERT INTO h_est SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 60, number FROM numbers(30000);
INSERT INTO h_real SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 60, number FROM numbers(30000);
CREATE HYPOTHETICAL PROJECTION p_h ON h_est (SELECT toStartOfHour(ts), sum(v) GROUP BY toStartOfHour(ts));
SELECT '-- a key expression';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT toStartOfHour(ts) AS h, sum(v) FROM h_est WHERE toStartOfHour(ts) < toDateTime('2026-01-02 00:00:00', 'UTC') GROUP BY h)
WHERE match(line, '^(Baseline|With |marks:|rows:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT toStartOfHour(ts) AS h, sum(v) FROM h_real WHERE toStartOfHour(ts) < toDateTime('2026-01-02 00:00:00', 'UTC') GROUP BY h);

DROP TABLE c_est;
DROP TABLE c_real;
DROP TABLE h_est;
DROP TABLE h_real;
DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE w_est;
DROP TABLE w_real;
DROP TABLE e_est;
DROP TABLE e_real;
DROP TABLE u_est;
