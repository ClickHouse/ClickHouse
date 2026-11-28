-- With `session_timezone` and `analyzer_compatibility_prefer_alias_over_subcolumn`, a compound identifier such as `t.event` in a
-- mutation names the column `event` of the table `t` in the query, but the subcolumn `event` of the column `t` in a background
-- mutation of `MergeTree`, which runs with the settings of the server. A string literal compared with it is read as the type of
-- what it names where the mutation runs, so it deletes the rows that the condition selects there.

SET session_timezone = 'Asia/Tokyo';
SET analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SET mutations_sync = 2;

-- A `DateTime` column and a `DateTime64(3)` subcolumn: the literal is read with the precision of the subcolumn in the background.
SELECT '--- a subcolumn with another precision than the column';
DROP TABLE IF EXISTS t_sub;
CREATE TABLE t_sub (id UInt8, event DateTime, t_sub Tuple(event DateTime64(3))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_sub VALUES (1, '2020-01-01 00:00:00', tuple('2020-01-01 00:00:00.500')), (2, '2020-01-01 00:00:01', tuple('2020-01-01 00:00:00.000'));
ALTER TABLE t_sub DELETE WHERE t_sub.event = '2020-01-01 00:00:00.500';
SELECT extract(command, 'event = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_sub';
SELECT id FROM t_sub ORDER BY id;
DROP TABLE t_sub;

-- A `String` subcolumn: the literal stays a string, and the background mutation compares it with the subcolumn.
SELECT '--- a String subcolumn';
DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (id UInt8, event DateTime, t_str Tuple(event String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_str VALUES (1, '2020-01-01 00:00:00', tuple('2020-01-01 00:00:00')), (2, '2020-01-01 00:00:00', tuple('x'));
ALTER TABLE t_str DELETE WHERE t_str.event = '2020-01-01 00:00:00';
SELECT extract(command, 'event = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_str';
SELECT id FROM t_str ORDER BY id;
DROP TABLE t_str;

-- A `Memory` table runs the mutation in the session, where the identifier names the column.
SELECT '--- a Memory table, which runs the mutation in the session';
DROP TABLE IF EXISTS t_mem;
CREATE TABLE t_mem (id UInt8, event DateTime, t_mem Tuple(event DateTime64(3))) ENGINE = Memory;
INSERT INTO t_mem VALUES (1, '2020-01-01 00:00:00', tuple('2020-01-01 00:00:01.000')), (2, '2020-01-01 00:00:01', tuple('2020-01-01 00:00:00.000'));
SELECT id FROM t_mem WHERE t_mem.event = '2020-01-01 00:00:00';
ALTER TABLE t_mem DELETE WHERE t_mem.event = '2020-01-01 00:00:00';
SELECT id FROM t_mem ORDER BY id;
DROP TABLE t_mem;
