-- With `session_timezone` and `prefer_column_name_to_alias`, an identifier with the name of an alias in a mutation names the
-- column in the query, but the alias in a background mutation of `MergeTree`, which runs with the settings of the server. A
-- string literal compared with it gets the time zone of the session (05459) if both are a `DateTime` without a time zone.

SET session_timezone = 'Asia/Tokyo';
SET prefer_column_name_to_alias = 1;
SET mutations_sync = 2;

SELECT '--- an alias of a DateTime expression with the name of a DateTime column';
DROP TABLE IF EXISTS t_prefer_temporal;
CREATE TABLE t_prefer_temporal (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_prefer_temporal VALUES (1, 1577804400), (2, 1577836800);
SELECT id FROM t_prefer_temporal WHERE isNotNull(identity(event) AS event) AND event = '2020-01-01 00:00:00';
ALTER TABLE t_prefer_temporal DELETE WHERE isNotNull(identity(event) AS event) AND event = '2020-01-01 00:00:00';
SELECT extract(command, 'event = [^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_prefer_temporal';
SELECT id FROM t_prefer_temporal ORDER BY id;

-- The expression of `event` refers to `other`, which names the `DateTime` column in the query but the `String` alias in the
-- background mutation, so the type of `event` there is not known: the comparison keeps its string and the mutation completes.
SELECT '--- an alias that refers to another alias with the name of a column';
DROP TABLE IF EXISTS t_prefer_other;
CREATE TABLE t_prefer_other (id UInt8, event DateTime, other DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_prefer_other VALUES (1, 1577804400, 1577804400);
ALTER TABLE t_prefer_other DELETE WHERE isNotNull(identity(other) AS event) AND isNotNull(toString(id) AS other)
    AND event = '2020-01-01 00:00:00';
SELECT extract(command, 'event = [^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_prefer_other';
SELECT id FROM t_prefer_other ORDER BY id;

-- A `Memory` table runs the mutation in the session, where the identifier names the `DateTime64(3)` column, while the alias is
-- a `DateTime`: a conversion with the type of the alias would drop the fraction of a second, so the literal is converted with
-- the precision of the column.
SELECT '--- an alias with another precision than the column';
DROP TABLE IF EXISTS t_prefer_precision;
CREATE TABLE t_prefer_precision (id UInt8, event DateTime64(3)) ENGINE = Memory;
INSERT INTO t_prefer_precision VALUES (1, '2020-01-01 00:00:00.500'), (2, '2020-01-01 00:00:00.000');
ALTER TABLE t_prefer_precision DELETE WHERE isNotNull(toDateTime(event) AS event) AND event = '2020-01-01 00:00:00.500';
SELECT id FROM t_prefer_precision ORDER BY id;

-- The same for an element of a tuple: `n.ts` names the `DateTime64(3)` element of the column, but the `DateTime` one of the
-- alias.
SELECT '--- an alias of a tuple with another precision of an element than the column';
DROP TABLE IF EXISTS t_prefer_tuple;
CREATE TABLE t_prefer_tuple (id UInt8, n Tuple(ts DateTime64(3))) ENGINE = Memory;
INSERT INTO t_prefer_tuple SELECT 1, tuple('2020-01-01 00:00:00.500') UNION ALL SELECT 2, tuple('2020-01-01 00:00:00.000');
ALTER TABLE t_prefer_tuple DELETE WHERE isNotNull(CAST(n, 'Tuple(ts DateTime)') AS n) AND n.ts = '2020-01-01 00:00:00.500';
SELECT id FROM t_prefer_tuple ORDER BY id;

-- The literal is converted with the precision of the element of the column, so the background mutation of `MergeTree`, where
-- `n.ts` names the `DateTime` element of the alias, gets the time zone too and deletes the right row.
SELECT '--- the same for MergeTree';
DROP TABLE IF EXISTS t_prefer_tuple_merge_tree;
CREATE TABLE t_prefer_tuple_merge_tree (id UInt8, n Tuple(ts DateTime64(3))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_prefer_tuple_merge_tree SELECT 1, tuple('2020-01-01 00:00:00') UNION ALL SELECT 2, tuple('2020-01-01 09:00:00');
ALTER TABLE t_prefer_tuple_merge_tree DELETE WHERE isNotNull(CAST(n, 'Tuple(ts DateTime)') AS n) AND n.ts = '2020-01-01 00:00:00';
SELECT extract(command, 'n\\.ts = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_prefer_tuple_merge_tree';
SELECT id FROM t_prefer_tuple_merge_tree ORDER BY id;

-- The analysis of the expression of an alias with the name of a column keeps the aliases inside its lambdas and subqueries.
SELECT '--- aliases inside the expression of an alias with the name of a column';
DROP TABLE IF EXISTS t_prefer_nested;
CREATE TABLE t_prefer_nested (id UInt8, flag UInt8, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO t_prefer_nested VALUES (1, 1, [1]), (2, 1, [2]), (3, 0, [3]);
ALTER TABLE t_prefer_nested DELETE WHERE (arrayExists(x -> isNotNull(identity(x) AS y) AND y = x, arr) AS flag) AND flag = 1 AND id = 1;
ALTER TABLE t_prefer_nested DELETE WHERE ((SELECT identity(1) AS y WHERE y = 1) AS flag) = 1 AND flag = 1 AND id = 2;
SELECT id FROM t_prefer_nested ORDER BY id;

DROP TABLE t_prefer_temporal;
DROP TABLE t_prefer_other;
DROP TABLE t_prefer_precision;
DROP TABLE t_prefer_tuple;
DROP TABLE t_prefer_tuple_merge_tree;
DROP TABLE t_prefer_nested;
