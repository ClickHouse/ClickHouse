-- Tags: shard

-- The tables named in the query of `view` without a database are read from the current database of the query, also when the
-- query is sent to other servers, whose own default database is different.

CREATE TABLE t_view_implicit_db (x UInt64) ENGINE = Memory;
INSERT INTO t_view_implicit_db VALUES (2), (3);
CREATE TABLE t_view_implicit_db_arr (x UInt64, arr Array(UInt64)) ENGINE = Memory;
INSERT INTO t_view_implicit_db_arr VALUES (2, [2, 3]), (3, [2, 3]), (5, [2, 3]);

SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db))
    SETTINGS serialize_query_plan = 1;
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM loop(view(SELECT x FROM t_view_implicit_db)) LIMIT 2);
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM view(SELECT x FROM view(SELECT x FROM t_view_implicit_db)));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(WITH c AS (SELECT x FROM t_view_implicit_db) SELECT x FROM c));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db_arr WHERE x IN arr));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db_arr WHERE x IN t_view_implicit_db));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db_arr WHERE x <= (SELECT max(x) FROM t_view_implicit_db)));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(SELECT arrayJoin((SELECT groupArray(x) FROM t_view_implicit_db)) AS x));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(WITH c AS (SELECT arrayJoin((SELECT groupArray(x) FROM t_view_implicit_db)) AS x) SELECT x FROM c));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(WITH (SELECT max(x) FROM t_view_implicit_db) AS m SELECT x FROM t_view_implicit_db_arr WHERE x <= m));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(WITH c AS MATERIALIZED (SELECT x FROM t_view_implicit_db) SELECT x FROM c))
SETTINGS enable_materialized_cte = 1;
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db WHERE 1 OR (SELECT count() FROM t_view_implicit_db) > 0))
SETTINGS enable_function_early_short_circuit = 1;
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(WITH t_view_implicit_db AS s SELECT x FROM t_view_implicit_db_arr WHERE x IN s));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db_arr WHERE 1 OR x IN t_view_implicit_db OR x IN (SELECT x FROM t_view_implicit_db)));

SELECT sum(l.x) FROM remote('127.0.0.2', numbers(4)) AS r INNER JOIN view(SELECT x FROM t_view_implicit_db) AS l ON r.number = l.x;
SELECT sum(l.x) FROM remote('127.0.0.2', numbers(4)) AS r
INNER JOIN view(WITH c AS MATERIALIZED (SELECT x FROM t_view_implicit_db) SELECT x FROM c UNION ALL SELECT x FROM c) AS l ON r.number = l.x
SETTINGS enable_materialized_cte = 1;

-- The condition is inside an aggregate function, evaluated on two shards.
SELECT countIf(number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db))) FROM remote('127.0.0.{2,3}', numbers(4));

SELECT sum(x) FROM view(SELECT x FROM t_view_implicit_db);

-- An `IN` table operand written with an alias and the references to the alias are sent as written: `db.t AS s` cannot
-- define the table alias `s`. The table `s` of the other `UNION` branch is not a reference to that alias.
CREATE TABLE s (x UInt64) ENGINE = Memory;
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM view(SELECT x FROM t_view_implicit_db_arr WHERE x IN (t_view_implicit_db AS s) AND x IN s
    UNION ALL SELECT x FROM t_view_implicit_db_arr WHERE x IN s))
SETTINGS serialize_query_plan = 0, log_comment = '05784_aliased_in_operand'; -- { serverError UNKNOWN_IDENTIFIER }
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0 FROM system.query_log
WHERE event_date >= yesterday() AND NOT is_initial_query AND position(query, '(`x` IN (`t_view_implicit_db` AS `s`)) AND (`x` IN (`s`))') > 0
    AND position(query, concat('`x` IN (`', currentDatabase(), '`.`s`)')) > 0
    AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '05784_aliased_in_operand');

-- The table of remote() is resolved by the remote server, in its own default database.
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4))
WHERE number IN (SELECT x FROM remote('127.0.0.3', view(SELECT x FROM t_view_implicit_db))); -- { serverError UNKNOWN_TABLE }
