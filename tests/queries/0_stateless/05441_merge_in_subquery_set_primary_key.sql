-- An `IN` subquery reading a `Merge` table must still use the primary key and the partition key of its tables
-- when the outer read builds the set for index analysis or PREWHERE.

DROP TABLE IF EXISTS t_merge_in_subquery;
DROP TABLE IF EXISTS m_merge_in_subquery;

CREATE TABLE t_merge_in_subquery (spoken_name String, event_date Date, event_time DateTime, tables Array(String))
ENGINE = MergeTree PARTITION BY event_date ORDER BY (spoken_name, event_date, event_time);
INSERT INTO t_merge_in_subquery VALUES ('a', '2026-10-16', '2026-10-16 10:00:00', ['x']), ('b', '2026-10-16', '2026-10-16 11:00:00', ['y']);
CREATE TABLE m_merge_in_subquery AS t_merge_in_subquery ENGINE = Merge(currentDatabase(), '^t_merge_in_subquery$');

-- The set is built for PREWHERE: `tables` is not a key column.
SELECT count() FROM m_merge_in_subquery
WHERE spoken_name = 'a' AND event_date = '2026-10-16'
    AND tables IN (SELECT tables FROM m_merge_in_subquery WHERE spoken_name = 'a' AND event_date = '2026-10-16')
SETTINGS force_primary_key = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, use_index_for_in_with_subqueries = 1;

SELECT count() FROM m_merge_in_subquery
WHERE spoken_name = 'a' AND event_date = '2026-10-16'
    AND tables IN (SELECT tables FROM m_merge_in_subquery WHERE spoken_name = 'a' AND event_date = '2026-10-16')
SETTINGS force_index_by_date = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, use_index_for_in_with_subqueries = 1;

-- The set is built for the primary key analysis of the outer read.
SELECT count() FROM t_merge_in_subquery
WHERE spoken_name IN (SELECT spoken_name FROM m_merge_in_subquery WHERE spoken_name = 'a' AND event_date = '2026-10-16')
SETTINGS force_primary_key = 1, use_index_for_in_with_subqueries = 1;

DROP TABLE m_merge_in_subquery;
DROP TABLE t_merge_in_subquery;
