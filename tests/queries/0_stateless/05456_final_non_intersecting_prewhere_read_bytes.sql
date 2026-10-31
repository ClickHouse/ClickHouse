-- With `optimize_move_to_prewhere_if_final_non_intersecting`, a FINAL query with a selective condition on a column outside
-- the sorting key reads the other columns only for the rows that pass it, on the key ranges that FINAL reads without merging.

DROP TABLE IF EXISTS t_final_prewhere_bytes;
CREATE TABLE t_final_prewhere_bytes (key UInt64, v UInt64, payload String, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY key
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_final_prewhere_bytes;
INSERT INTO t_final_prewhere_bytes SELECT number, number % 1000, repeat(toString(number), 20), 1 FROM numbers(100000);
SYSTEM START MERGES t_final_prewhere_bytes;
OPTIMIZE TABLE t_final_prewhere_bytes FINAL;
SYSTEM STOP MERGES t_final_prewhere_bytes;
INSERT INTO t_final_prewhere_bytes SELECT number, number % 1000, 'new', 2 FROM numbers(50000, 1000);

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, use_query_condition_cache = 0;

SELECT count(), sum(length(payload)) FROM t_final_prewhere_bytes FINAL WHERE v = 7
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0, log_comment = '05456_off';
SELECT count(), sum(length(payload)) FROM t_final_prewhere_bytes FINAL WHERE v = 7
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1, log_comment = '05456_on';

SYSTEM FLUSH LOGS query_log;

SELECT
    on.read_bytes * 5 < off.read_bytes,
    on.marks_non_intersecting > 0 AND on.marks_non_intersecting = off.marks_non_intersecting,
    on.marks_merged > 0 AND on.marks_merged = off.marks_merged
FROM
(
    SELECT read_bytes, ProfileEvents['SelectedMarksFinalNonIntersecting'] AS marks_non_intersecting,
        ProfileEvents['SelectedMarksFinalMerged'] AS marks_merged
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05456_on' AND event_date >= yesterday()
) AS on,
(
    SELECT read_bytes, ProfileEvents['SelectedMarksFinalNonIntersecting'] AS marks_non_intersecting,
        ProfileEvents['SelectedMarksFinalMerged'] AS marks_merged
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05456_off' AND event_date >= yesterday()
) AS off;

DROP TABLE t_final_prewhere_bytes;
