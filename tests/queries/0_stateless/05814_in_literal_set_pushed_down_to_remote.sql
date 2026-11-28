-- Tags: shard
-- A filter with a literal `IN` list is pushed to the remote server as an AST built from the set.

-- The predicate is pushed as an AST only when the remote query is sent as text.
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_in_push;
CREATE TABLE t_in_push (a UInt32, s String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_in_push SELECT number, toString(number) FROM numbers(1000);

SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_in_push)) WHERE a IN (3, 5, 7, 300) SETTINGS log_comment = '05814_in';
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_in_push)) WHERE s NOT IN tuple('3', '5', 'x') SETTINGS log_comment = '05814_not_in';
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_in_push)) WHERE (a, s) IN [(1, '1'), (2, '3')] SETTINGS log_comment = '05814_array';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, countIf(query LIKE '%HAVING in(`a`, %(3, 5, 7, 300)%')
    + countIf(query LIKE '%HAVING notIn(`s`, %(\'3\', \'5\', \'x\')%')
    + countIf(query LIKE '%HAVING in(tuple(`a`, `s`), %[(1, \'1\'), (2, \'3\')]%')
FROM system.query_log
-- The secondary queries do not carry `current_database`, so match them by the database they read.
WHERE has(databases, currentDatabase())
    AND event_date >= yesterday() AND event_time > now() - 600 AND type = 'QueryFinish' AND is_initial_query = 0
    AND log_comment IN ('05814_in', '05814_not_in', '05814_array') AND query LIKE '%t_in_push%'
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_in_push;
