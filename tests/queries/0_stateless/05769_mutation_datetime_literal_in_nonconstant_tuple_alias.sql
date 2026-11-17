-- With `session_timezone`, a string literal in the tuple of `IN` with an identifier on the left is read as that identifier's
-- `DateTime` type in the session time zone. With an element that is not a constant, the analyzer casts the elements without an
-- AST. If the identifier names an alias of another temporal type than the column with its name, which one it names depends on
-- `prefer_column_name_to_alias`, so the literal is converted for the type it has where the mutation runs.
-- https://github.com/ClickHouse/ClickHouse/issues/123959

SET session_timezone = 'Asia/Tokyo';
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_in_alias;
CREATE TABLE t_in_alias (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;

SELECT 'prefer_column_name_to_alias = 0';
-- 1577804400 is 2020-01-01 00:00:00 in Tokyo, 1577836800 is 2020-01-01 09:00:00 there.
INSERT INTO t_in_alias VALUES (1, 1577804400), (2, 1577836800);
SELECT id FROM t_in_alias
WHERE isNotNull(toDateTime64(event, 3) AS event) AND event IN tuple('2020-01-01 00:00:00', toDateTime(id));
ALTER TABLE t_in_alias DELETE
WHERE isNotNull(toDateTime64(event, 3) AS event) AND event IN tuple('2020-01-01 00:00:00', toDateTime(id));
SELECT id FROM t_in_alias ORDER BY id;

SELECT 'prefer_column_name_to_alias = 1';
TRUNCATE TABLE t_in_alias;
SET prefer_column_name_to_alias = 1;
INSERT INTO t_in_alias VALUES (1, 1577804400), (2, 1577836800);
SELECT id FROM t_in_alias
WHERE isNotNull(toDateTime64(event, 3) AS event) AND event IN tuple('2020-01-01 00:00:00', toDateTime(id));
ALTER TABLE t_in_alias DELETE
WHERE isNotNull(toDateTime64(event, 3) AS event) AND event IN tuple('2020-01-01 00:00:00', toDateTime(id));
SELECT id FROM t_in_alias ORDER BY id;

DROP TABLE t_in_alias;
