-- A hash join does not build the row store when both inputs are sorted by the join key.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET max_rows_in_set_to_optimize_join = 0;
SET enable_hash_join_row_store = 1;
SET min_rows_ratio_for_hash_join_row_store = 0;
SET collect_hash_table_stats_during_joins = 0;
SET allow_experimental_join_right_table_sorting = 0;

DROP TABLE IF EXISTS sorted_probe;
DROP TABLE IF EXISTS sorted_build;
DROP TABLE IF EXISTS unsorted_build;

CREATE TABLE sorted_probe (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE sorted_build (k UInt64, a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE unsorted_build (k UInt64, a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO sorted_probe SELECT number % 100, number FROM numbers(1000);
INSERT INTO sorted_build SELECT number, number * 2, if(number % 10 = 0, NULL, number * 3) FROM numbers(100);
INSERT INTO unsorted_build SELECT * FROM sorted_build;

SELECT count(), sum(a), sum(b) FROM sorted_probe AS l INNER JOIN sorted_build AS r ON l.k = r.k
SETTINGS log_comment = '05814_sorted';

SELECT count(), sum(a), sum(b) FROM sorted_probe AS l LEFT JOIN sorted_build AS r ON l.k = r.k
SETTINGS log_comment = '05814_sorted_left';

SELECT count(), sum(a), sum(b) FROM sorted_probe AS l INNER JOIN unsorted_build AS r ON l.k = r.k
SETTINGS log_comment = '05814_unsorted';

SYSTEM FLUSH LOGS text_log, query_log;

SELECT log_comment, query_id IN (
    SELECT query_id FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND message_format_string = '{}Initialized Row store with {} columns')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND log_comment IN ('05814_sorted', '05814_sorted_left', '05814_unsorted') AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE sorted_probe;
DROP TABLE sorted_build;
DROP TABLE unsorted_build;
