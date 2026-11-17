-- With `session_timezone`, a string literal compared with or assigned to a `DateTime` column of a mutation is converted. An
-- identifier that names the alias of the literal is converted at its own position for such a use, so the alias keeps naming the
-- string for the other uses, and both comparisons or both assignments read it in the session time zone.
-- https://github.com/ClickHouse/ClickHouse/issues/123959

SET session_timezone = 'Asia/Tokyo';
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_alias_reference;
CREATE TABLE t_alias_reference (id UInt8, event DateTime, other DateTime, s String, v UInt8) ENGINE = MergeTree ORDER BY id;
-- 1577804400 is 2020-01-01 00:00:00 in Tokyo, 1577836800 is 2020-01-01 09:00:00 there.
INSERT INTO t_alias_reference VALUES (1, 1577804400, 1577804400, '2020-01-01 00:00:00', 0), (2, 1577804400, 1577836800, '2020-01-01 00:00:00', 0);

SELECT 'two comparisons';
SELECT id FROM t_alias_reference WHERE event = ('2020-01-01 00:00:00' AS lit) AND other = lit;
ALTER TABLE t_alias_reference UPDATE v = 1 WHERE event = ('2020-01-01 00:00:00' AS lit) AND other = lit;
SELECT id, v FROM t_alias_reference ORDER BY id;

SELECT 'a comparison with a string keeps the string';
ALTER TABLE t_alias_reference UPDATE v = v + 10 WHERE event = ('2020-01-01 00:00:00' AS lit) AND s = lit;
SELECT id, v FROM t_alias_reference ORDER BY id;

SELECT 'a constant tuple of IN';
ALTER TABLE t_alias_reference UPDATE v = v + 100 WHERE event = ('2020-01-01 00:00:00' AS lit) AND other IN tuple(lit, '2020-01-01 09:00:00');
SELECT id, v FROM t_alias_reference ORDER BY id;

SELECT 'two assignments';
ALTER TABLE t_alias_reference UPDATE event = ('2020-01-02 00:00:00' AS lit), other = lit WHERE id = 2;
SELECT id, toUnixTimestamp(event), toUnixTimestamp(other) FROM t_alias_reference ORDER BY id;

SELECT 'a delete over two comparisons';
ALTER TABLE t_alias_reference DELETE WHERE event = ('2020-01-01 00:00:00' AS lit) AND other = lit;
SELECT id FROM t_alias_reference ORDER BY id;

DROP TABLE t_alias_reference;

-- An alias named like a column names the column with the other value of `prefer_column_name_to_alias`, so an identifier that
-- names it is left as it is, while the literal itself is converted.
SELECT 'an alias named like a column';
DROP TABLE IF EXISTS t_alias_named_like_column;
CREATE TABLE t_alias_named_like_column (id UInt8, event DateTime, other DateTime, c String, v UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_named_like_column VALUES (1, 1577804400, 1577804400, 'x', 0);
ALTER TABLE t_alias_named_like_column UPDATE v = 1 WHERE event = ('2020-01-01 00:00:00' AS c) AND other = c;
SELECT position(command, ' AS c, ') > 0, position(command, 'other = c)') > 0
FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_named_like_column';
DROP TABLE t_alias_named_like_column;
