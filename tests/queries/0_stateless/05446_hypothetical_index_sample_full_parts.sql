-- past `index_scan_budget_rows` the draws decide which parts the sample reads fully.
-- so these parts count with their draws as the other parts do, and their exact counts do not bias the estimate

-- part statistics on v would remove the last part from the baseline
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_full_parts;
CREATE TABLE t_full_parts (a UInt64, b UInt64, v UInt8) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_full_parts;
-- 30 parts of 16 marks: a window of 12 marks with v = 0 and a window of 4 marks with v = 1
INSERT INTO t_full_parts SELECT intDiv(number % 16, 12), 0, number % 16 >= 12 FROM numbers(480)
    SETTINGS max_block_size = 16, max_insert_block_size = 16, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
-- a part of 120 windows with v = 1. The baseline keeps 2 marks of each window
INSERT INTO t_full_parts SELECT intDiv(number, 12), number % 12, 1 FROM numbers(1440);
CREATE HYPOTHETICAL INDEX idx_v ON t_full_parts (v) TYPE minmax GRANULARITY 12;

-- all parts take one draw from every 8 baseline marks. The first draw of a small part always reads its 12-mark window,
-- and the second draw reads the 4-mark window with a chance of 1/2. When the parts that read both windows counted exactly,
-- the other parts took 16 surviving marks each, and the span missed the real granules
SELECT
    (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_full_parts' AND active) AS parts,
    extract(sampled, 'sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(sampled, 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    toUInt64(extract(full, 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)')) AS real,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM
(
    SELECT
        (SELECT arrayStringConcat(groupArray(explain), '\n')
         FROM (EXPLAIN WHATIF index_scan_budget_rows = 960 SELECT a FROM t_full_parts WHERE b = 0 AND v = 0)) AS sampled,
        (SELECT arrayStringConcat(groupArray(explain), '\n')
         FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT a FROM t_full_parts WHERE b = 0 AND v = 0)) AS full
);

DROP TABLE t_full_parts;
