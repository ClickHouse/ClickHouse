-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- A subcolumn of a column returned by a subquery is read alone: the query reads far less than the whole column.

SET enable_analyzer = 1, query_plan_enable_optimizations = 1, serialize_query_plan = 0;

DROP TABLE IF EXISTS t_io;
CREATE TABLE t_io (id UInt32, tup Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_io SELECT number, (number, randomPrintableASCII(200)) FROM numbers(100000);

SELECT sum(tup.a) FROM (SELECT * FROM t_io) SETTINGS query_plan_push_down_volume_reducing_functions = 0, log_comment = '05508_off';
SELECT sum(tup.a) FROM (SELECT * FROM t_io) SETTINGS query_plan_push_down_volume_reducing_functions = 1, log_comment = '05508_on';

SYSTEM FLUSH LOGS query_log;
SELECT
    sumIf(read_bytes, log_comment = '05508_on') * 10 < sumIf(read_bytes, log_comment = '05508_off')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05508_on', '05508_off')
    AND event_date >= yesterday();

DROP TABLE t_io;
