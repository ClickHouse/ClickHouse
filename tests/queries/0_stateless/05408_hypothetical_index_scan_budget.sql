-- past `index_scan_budget_rows` an index estimate reads a sample of index windows.
-- the twin table with the materialized indexes gives the real granules, and the sampled span must hold them
-- a full scan is exact, as the twin table shows, so the later tables take it for the real granules

DROP TABLE IF EXISTS t_budget;
DROP TABLE IF EXISTS t_budget_real;

-- b follows the table order, c has a random value in each granule
CREATE TABLE t_budget (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
CREATE TABLE t_budget_real (a UInt64, b UInt64, c UInt64, INDEX idx_b b TYPE minmax GRANULARITY 1, INDEX idx_c c TYPE minmax GRANULARITY 2)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_budget SELECT number, intDiv(number, 100), cityHash64(intDiv(number, 100)) % 3 FROM numbers(12000);
INSERT INTO t_budget_real SELECT * FROM t_budget;

CREATE HYPOTHETICAL INDEX idx_b ON t_budget (b) TYPE minmax GRANULARITY 1;
CREATE HYPOTHETICAL INDEX idx_c ON t_budget (c) TYPE minmax GRANULARITY 2;

-- the real granules of the twin table, by predicate
CREATE TEMPORARY TABLE real_granules (predicate String, granules UInt64);
INSERT INTO real_granules SELECT 'b < 60', extract(arrayStringConcat(groupArray(explain), '\n'), '(?s).*Granules: (\\d+)/')
FROM (EXPLAIN indexes = 1 SELECT a FROM t_budget_real WHERE b < 60 SETTINGS use_skip_indexes_on_data_read = 0);
INSERT INTO real_granules SELECT 'c = 0', extract(arrayStringConcat(groupArray(explain), '\n'), '(?s).*Granules: (\\d+)/')
FROM (EXPLAIN indexes = 1 SELECT a FROM t_budget_real WHERE c = 0 SETTINGS use_skip_indexes_on_data_read = 0);
INSERT INTO real_granules SELECT 'b < 60 AND c = 0', extract(arrayStringConcat(groupArray(explain), '\n'), '(?s).*Granules: (\\d+)/')
FROM (EXPLAIN indexes = 1 SELECT a FROM t_budget_real WHERE b < 60 AND c = 0 SETTINGS use_skip_indexes_on_data_read = 0);
INSERT INTO real_granules SELECT 'b = 42', extract(arrayStringConcat(groupArray(explain), '\n'), '(?s).*Granules: (\\d+)/')
FROM (EXPLAIN indexes = 1 SELECT a FROM t_budget_real WHERE b = 42 SETTINGS use_skip_indexes_on_data_read = 0);

SELECT '-- the real granules of the twin table';
SELECT * FROM real_granules ORDER BY predicate;

SELECT '-- a full scan reads every window, so the numbers are exact';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT a FROM t_budget WHERE b < 60 AND c = 0)
WHERE match(explain, '^(With |\\(combined)|^ +(marks|marks_span|skip_ratio|empirical_status|sampled_marks):');

-- the estimate, the span and whether the span holds the real granules
CREATE TEMPORARY TABLE sampled (candidate String, predicate String, report String);
INSERT INTO sampled SELECT pair.1, pair.2, w
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS w FROM (EXPLAIN WHATIF index_scan_budget_rows = 1000 SELECT a FROM t_budget WHERE b < 60 AND c = 0))
ARRAY JOIN [('idx_b', 'b < 60'), ('idx_c', 'c = 0'), ('(combined', 'b < 60 AND c = 0')] AS pair;
INSERT INTO sampled SELECT 'idx_b', 'b = 42', arrayStringConcat(groupArray(explain), '\n')
FROM (EXPLAIN WHATIF index_scan_budget_rows = 1000 SELECT a FROM t_budget WHERE b = 42);

SELECT '-- past the budget each index reads a sample of its windows, and the span holds the real granules';
SELECT
    sampled.predicate,
    candidate,
    extract(block, '(?s)^.*?sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(block, '^With [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    granules AS real,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM (SELECT *, substring(report, position(report, 'With ' || candidate || ' ')) AS block FROM sampled WHERE candidate != '(combined') AS sampled
INNER JOIN real_granules ON real_granules.predicate = sampled.predicate
ORDER BY sampled.predicate;

SELECT '-- a sample leaves the other windows unknown, so the combined row gets no estimate';
SELECT extract(report, '(?s)\\(combined: .*?empirical_status: +(\\w+)'), extract(report, '(?s)\\(combined: .*?empirical_reason: +([^\\n]+)')
FROM sampled
WHERE candidate = '(combined';

-- the baseline is every second granule. A GRANULARITY 2 window holds one baseline granule, a GRANULARITY 4 window holds two
SELECT '-- max_rows_to_read lowers the budget, and a limit below the smallest sample leaves the estimate unsupported';
CREATE HYPOTHETICAL INDEX idx_d ON t_budget (c) TYPE minmax GRANULARITY 4;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF SELECT a FROM t_budget WHERE a IN (SELECT number * 200 + 50 FROM numbers(60)) AND c = 0 SETTINGS max_rows_to_read = 6000)
WHERE match(explain, '^With idx_[cd]|^ +(marks|empirical_status|empirical_reason|sampled_marks):');

-- each part has 101 windows, so a step of 10 leaves the last window alone in its run. Only the last window of each part survives
SELECT '-- the window of a shorter last run stands for fewer windows, so the estimate holds the real granules';
DROP TABLE IF EXISTS t_tail;
CREATE TABLE t_tail (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_tail;
INSERT INTO t_tail SELECT number FROM numbers(10100);
INSERT INTO t_tail SELECT number FROM numbers(10100);
INSERT INTO t_tail SELECT number FROM numbers(10100);
CREATE HYPOTHETICAL INDEX idx_x ON t_tail (x) TYPE minmax GRANULARITY 1;
SELECT
    extract(report, 'sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(report, 'With idx_x [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    real,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS report
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 3030 SELECT x FROM t_tail WHERE x >= 10000)
) AS estimated,
(
    SELECT toUInt64(extract(arrayStringConcat(groupArray(explain), '\n'), 'With idx_x [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)')) AS real
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT x FROM t_tail WHERE x >= 10000)
) AS exact;

DROP TABLE t_tail;

-- each part has a window of 8 marks and a window of 1 mark, and a step of 2 samples one of them. Only the 8-mark windows survive
SELECT '-- windows with different marks count with their marks, so the estimate holds the real granules';
DROP TABLE IF EXISTS t_uneven;
CREATE TABLE t_uneven (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_uneven;
-- 30 parts of 900 rows
INSERT INTO t_uneven SELECT number % 900 FROM numbers(27000)
    SETTINGS max_block_size = 900, max_insert_block_size = 900, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
CREATE HYPOTHETICAL INDEX idx_x ON t_uneven (x) TYPE minmax GRANULARITY 8;
SELECT
    (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_uneven' AND active) AS parts,
    extract(report, 'sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(report, 'With idx_x [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    real,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS report
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 13500 SELECT x FROM t_uneven WHERE x < 800)
) AS estimated,
(
    SELECT toUInt64(extract(arrayStringConcat(groupArray(explain), '\n'), 'With idx_x [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)')) AS real
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT x FROM t_uneven WHERE x < 800)
) AS exact;

DROP TABLE t_uneven;

-- the baseline keeps every mark of each twentieth window and two marks of each other window. Only the full windows have v < 1000
SELECT '-- a window joins the sample with a chance in proportion to its baseline marks, so the span holds the real granules';
DROP TABLE IF EXISTS t_mass;
CREATE TABLE t_mass (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_mass SELECT intDiv(number, 16) AS w, if(w % 20 = 5, 0, number % 16), if(w % 20 = 5, w, 1000 + w) FROM numbers(960);
CREATE HYPOTHETICAL INDEX idx_v ON t_mass (v) TYPE minmax GRANULARITY 16;
SELECT
    extract(report, 'sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(report, 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    real,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS report
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 480 SELECT a FROM t_mass WHERE b = 0 AND v < 1000)
) AS estimated,
(
    SELECT toUInt64(extract(arrayStringConcat(groupArray(explain), '\n'), 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)')) AS real
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT a FROM t_mass WHERE b = 0 AND v < 1000)
) AS exact;

-- only the full window 25 has v = 25. The sample reads it, so its 16 marks survive for v = 25 and are skipped for v != 25
SELECT '-- the sampled windows bound the estimate, so the estimate is in the span';
SELECT
    predicate,
    extract(report, 'sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(report, 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    real,
    toUInt64(estimate[1]) BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS estimate_in_span,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM
(
    SELECT 'v = 25' AS predicate, arrayStringConcat(groupArray(explain), '\n') AS report
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 480 SELECT a FROM t_mass WHERE b = 0 AND v = 25)
    UNION ALL
    SELECT 'v != 25', arrayStringConcat(groupArray(explain), '\n')
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 480 SELECT a FROM t_mass WHERE b = 0 AND v != 25)
) AS estimated
INNER JOIN
(
    SELECT 'v = 25' AS predicate, toUInt64(extract(arrayStringConcat(groupArray(explain), '\n'), 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)')) AS real
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT a FROM t_mass WHERE b = 0 AND v = 25)
    UNION ALL
    SELECT 'v != 25', toUInt64(extract(arrayStringConcat(groupArray(explain), '\n'), 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)'))
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT a FROM t_mass WHERE b = 0 AND v != 25)
) AS exact USING (predicate)
ORDER BY predicate;

DROP TABLE t_mass;

-- a part of two windows keeps its 64 marks in one draw, and only its second window has v = 2.
-- the other part keeps 2 marks of each of its 58 windows, in draws of 4 marks, and only its window 50 has v = 1
SELECT '-- draws of unequal weights count as fewer draws, so the span holds the real granules';
-- part statistics on v would remove the parts with no surviving window from the baseline
SET use_statistics_for_part_pruning = 0;
DROP TABLE IF EXISTS t_weight;
CREATE TABLE t_weight (a UInt64, b UInt64, v UInt8) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_weight;
INSERT INTO t_weight SELECT intDiv(number, 32), 0, if(number >= 32, 2, 0) FROM numbers(64);
INSERT INTO t_weight SELECT intDiv(number, 32), number % 32, number = 1600 FROM numbers(1856);
CREATE HYPOTHETICAL INDEX idx_v ON t_weight (v) TYPE minmax GRANULARITY 32;
SELECT
    predicate,
    extract(report, 'sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(report, 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    real,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM
(
    SELECT 'v = 2' AS predicate, arrayStringConcat(groupArray(explain), '\n') AS report
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 960 SELECT a FROM t_weight WHERE b = 0 AND v = 2)
    UNION ALL
    SELECT 'v >= 1', arrayStringConcat(groupArray(explain), '\n')
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 960 SELECT a FROM t_weight WHERE b = 0 AND v >= 1)
) AS estimated
INNER JOIN
(
    SELECT 'v = 2' AS predicate, toUInt64(extract(arrayStringConcat(groupArray(explain), '\n'), 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)')) AS real
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT a FROM t_weight WHERE b = 0 AND v = 2)
    UNION ALL
    SELECT 'v >= 1', toUInt64(extract(arrayStringConcat(groupArray(explain), '\n'), 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)'))
    FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT a FROM t_weight WHERE b = 0 AND v >= 1)
) AS exact USING (predicate)
ORDER BY predicate;

DROP TABLE t_weight;
DROP TABLE t_budget;
DROP TABLE t_budget_real;
