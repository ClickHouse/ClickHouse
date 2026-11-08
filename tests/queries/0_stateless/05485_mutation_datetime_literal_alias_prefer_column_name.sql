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

DROP TABLE t_prefer_temporal;
DROP TABLE t_prefer_other;
