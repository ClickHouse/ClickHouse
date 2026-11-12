-- past `index_scan_budget_rows` a part takes a draw from every run of baseline marks, and its last run can be shorter.
-- a heavy draw next to a light draw keeps all its variance, so the span holds the real granules

DROP TABLE IF EXISTS t_mixed;
CREATE TABLE t_mixed (x UInt64, v UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_mixed;
-- 30 parts of 9 marks: 4 of the first 8 marks have v = 1, and the last mark has v = 0
INSERT INTO t_mixed SELECT number % 9, number % 9 IN (2, 3, 4, 6) FROM numbers(270)
    SETTINGS max_block_size = 9, max_insert_block_size = 9, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
CREATE HYPOTHETICAL INDEX idx_v ON t_mixed (v) TYPE minmax GRANULARITY 1;

-- each part takes one draw of 8 marks from its first 8 marks and one draw of 1 mark from its last mark.
-- 9 of the 30 heavy draws find v = 1. When each difference of successive draws counted with the mean weight of its two draws,
-- the light draws hid a part of the variance of the heavy draws, and the span missed the real granules
SELECT
    (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_mixed' AND active) AS parts,
    extract(sampled, 'sampled_marks: +(\\d+ / \\d+)') AS sampled_marks,
    extractGroups(sampled, 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)\\n +marks_span: +(\\d+) to (\\d+)') AS estimate,
    toUInt64(extract(full, 'With idx_v [^\\n]*\\n +status: +applicable\\n +marks: +(\\d+)')) AS real,
    real BETWEEN toUInt64(estimate[2]) AND toUInt64(estimate[3]) AS real_in_span
FROM
(
    SELECT
        (SELECT arrayStringConcat(groupArray(explain), '\n')
         FROM (EXPLAIN WHATIF index_scan_budget_rows = 34 SELECT x FROM t_mixed WHERE v = 1)) AS sampled,
        (SELECT arrayStringConcat(groupArray(explain), '\n')
         FROM (EXPLAIN WHATIF index_scan_budget_rows = 0 SELECT x FROM t_mixed WHERE v = 1)) AS full
);

DROP TABLE t_mixed;
