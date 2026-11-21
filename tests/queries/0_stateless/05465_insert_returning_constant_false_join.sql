-- The insert of `INSERT ... RETURNING` was a part of the read of the inserted data in the `RETURNING` query, so an optimization
-- of its plan that removed that read also removed the insert: with a `JOIN` whose `ON` folds to false, the query reported
-- success and wrote nothing: https://github.com/ClickHouse/ClickHouse/issues/127723

DROP TABLE IF EXISTS t_returning_false_join;
CREATE TABLE t_returning_false_join (x UInt64) ENGINE = MergeTree ORDER BY x;

SET query_plan_short_circuit_constant_false_join = 1;

INSERT INTO t_returning_false_join RETURNING (SELECT t.x FROM t_returning_false_join AS t INNER JOIN system.one AS o ON t.x = o.dummy AND 1 = 0) VALUES (1), (2), (3);
SELECT 'inner', groupArray(x) FROM (SELECT x FROM t_returning_false_join ORDER BY x);
TRUNCATE TABLE t_returning_false_join;

INSERT INTO t_returning_false_join RETURNING (SELECT count() FROM system.one AS o LEFT JOIN t_returning_false_join AS t ON t.x = o.dummy AND 1 = 0) VALUES (1), (2), (3);
SELECT 'left', groupArray(x) FROM (SELECT x FROM t_returning_false_join ORDER BY x);
TRUNCATE TABLE t_returning_false_join;

INSERT INTO t_returning_false_join RETURNING (SELECT t.x FROM t_returning_false_join AS t INNER JOIN system.one AS o ON o.dummy = 1 AND o.dummy = 2) VALUES (1), (2), (3);
SELECT 'contradiction', groupArray(x) FROM (SELECT x FROM t_returning_false_join ORDER BY x);

DROP TABLE t_returning_false_join;
