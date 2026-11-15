-- A Merge child plan that runs distributed returns one interleaved stream, so it must not be read in order.

DROP TABLE IF EXISTS t_merge_dp_order;
CREATE TABLE t_merge_dp_order (x Int32) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES t_merge_dp_order;
INSERT INTO t_merge_dp_order SELECT number * 4 FROM numbers(10);
INSERT INTO t_merge_dp_order SELECT number * 4 + 1 FROM numbers(10);
INSERT INTO t_merge_dp_order SELECT number * 4 + 2 FROM numbers(10);
INSERT INTO t_merge_dp_order SELECT number * 4 + 3 FROM numbers(10);
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_dp_order' AND active;

-- max_rows_to_group_by = 0: with the CI profile's limit the child plans are not distributed.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_workers_num = 4,
    optimize_read_in_order = 1, max_rows_to_group_by = 0;

SELECT groupArray(x) FROM (SELECT x FROM merge(currentDatabase(), '^t_merge_dp_order$') ORDER BY x)
    SETTINGS log_comment = '05565_order_by';
SELECT groupArray(x) FROM (SELECT x FROM merge(currentDatabase(), '^t_merge_dp_order$') ORDER BY x DESC LIMIT 5);
SELECT count() FROM t_merge_dp_order AS t1 INNER JOIN merge(currentDatabase(), '^t_merge_dp_order$') AS t2
    ON t1.x < t2.x AND t1.x > t2.x - 3 SETTINGS join_algorithm = 'ie_join', query_plan_join_swap_table = 0;
SELECT count() FROM t_merge_dp_order AS t1 INNER JOIN merge(currentDatabase(), '^t_merge_dp_order$') AS t2
    ON t1.x = t2.x SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;
SELECT count(), uniqExact(x) FROM (SELECT x, count() FROM merge(currentDatabase(), '^t_merge_dp_order$') GROUP BY x)
    SETTINGS optimize_aggregation_in_order = 1;
SELECT groupArray(rn) FROM (SELECT x, row_number() OVER (ORDER BY x) AS rn FROM merge(currentDatabase(), '^t_merge_dp_order$') ORDER BY x LIMIT 8)
    SETTINGS optimize_read_in_window_order = 1;

SYSTEM FLUSH LOGS query_log;
-- The child plan still runs distributed.
SELECT count() > 0 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND query = 'main'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
            AND log_comment = '05565_order_by')
SETTINGS make_distributed_plan = 0;

DROP TABLE t_merge_dp_order;
