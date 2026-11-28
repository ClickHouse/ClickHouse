-- With `session_timezone`, the predicate of a mutation is analyzed to convert its `DateTime` literals. The command is parsed
-- again from its text there, which leaves the set operations of its subqueries not normalized, so they are normalized as the
-- mutation itself normalizes them, also when no literal is converted.
-- https://github.com/ClickHouse/ClickHouse/issues/123959

SET session_timezone = 'Asia/Tokyo';
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_set_operation;
CREATE TABLE t_set_operation (id UInt8, event DateTime, v UInt16) ENGINE = MergeTree ORDER BY id;
-- 1577804400 is 2020-01-01 00:00:00 in Tokyo, 1577836800 is 2020-01-01 09:00:00 there.
INSERT INTO t_set_operation VALUES (1, 1577804400, 0), (2, 1577836800, 0), (3, 1577804400, 0);

ALTER TABLE t_set_operation UPDATE v = v + 1 WHERE id IN ((SELECT 2) UNION DISTINCT (SELECT 3));
ALTER TABLE t_set_operation UPDATE v = v + 10 WHERE id IN ((SELECT 1) EXCEPT (SELECT 3));
ALTER TABLE t_set_operation UPDATE v = v + 100 WHERE id IN ((SELECT 3) INTERSECT (SELECT 3));
-- A literal that is converted, next to a set operation.
ALTER TABLE t_set_operation UPDATE v = v + 1000 WHERE event = '2020-01-01 00:00:00' AND id IN ((SELECT 1) UNION ALL (SELECT 2));

SELECT id, v FROM t_set_operation ORDER BY id;

DROP TABLE t_set_operation;
