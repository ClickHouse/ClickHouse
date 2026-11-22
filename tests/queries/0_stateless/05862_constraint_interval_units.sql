-- intervals of different kinds compare after a conversion to one unit, so the graph does not compare their counts

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

DROP TABLE IF EXISTS t_interval_check;
DROP TABLE IF EXISTS t_interval_query;
DROP TABLE IF EXISTS t_interval_bridge;
DROP TABLE IF EXISTS t_interval_same;

CREATE TABLE t_interval_check (n Int64, CONSTRAINT c CHECK toIntervalMinute(n) = toIntervalSecond(60)) ENGINE = Memory;
INSERT INTO t_interval_check VALUES (1);
SELECT 'check', count() FROM t_interval_check WHERE toIntervalMinute(n) < toIntervalMinute(2) SETTINGS optimize_using_constraints = 0;
SELECT 'check', count() FROM t_interval_check WHERE toIntervalMinute(n) < toIntervalMinute(2) SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_interval_query (n Int64, CONSTRAINT c CHECK toIntervalSecond(n) = toIntervalSecond(60)) ENGINE = Memory;
INSERT INTO t_interval_query VALUES (60);
SELECT 'query', count() FROM t_interval_query WHERE toIntervalSecond(n) < toIntervalMinute(2) SETTINGS optimize_using_constraints = 0;
SELECT 'query', count() FROM t_interval_query WHERE toIntervalSecond(n) < toIntervalMinute(2) SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_interval_bridge (n Int64, CONSTRAINT c CHECK toIntervalMinute(n) = n AND n = toIntervalSecond(n)) ENGINE = Memory;
INSERT INTO t_interval_bridge VALUES (1);
SELECT 'bridge', count() FROM t_interval_bridge WHERE toIntervalMinute(n) = toIntervalSecond(n) SETTINGS optimize_using_constraints = 0;
SELECT 'bridge', count() FROM t_interval_bridge WHERE toIntervalMinute(n) = toIntervalSecond(n) SETTINGS optimize_using_constraints = 1;

-- intervals of one kind still use the bounds
CREATE TABLE t_interval_same (n Int64, CONSTRAINT c CHECK toIntervalMinute(n) = toIntervalMinute(1)) ENGINE = Memory;
INSERT INTO t_interval_same VALUES (1);
SELECT 'same', count() FROM t_interval_same WHERE toIntervalMinute(n) < toIntervalMinute(2) SETTINGS optimize_using_constraints = 0;
SELECT 'same', count() FROM t_interval_same WHERE toIntervalMinute(n) < toIntervalMinute(2) SETTINGS optimize_using_constraints = 1;
SELECT 'same equal constant', count() FROM t_interval_same WHERE toString(toIntervalMinute(n)) = '1' SETTINGS optimize_using_constraints = 0;
SELECT 'same equal constant', count() FROM t_interval_same WHERE toString(toIntervalMinute(n)) = '1' SETTINGS optimize_using_constraints = 1;
SELECT 'same folded', countIf(explain LIKE '%WHERE%') FROM (EXPLAIN QUERY TREE SELECT count() FROM t_interval_same WHERE toIntervalMinute(n) < toIntervalMinute(2) SETTINGS optimize_using_constraints = 1);

DROP TABLE t_interval_check;
DROP TABLE t_interval_query;
DROP TABLE t_interval_bridge;
DROP TABLE t_interval_same;
