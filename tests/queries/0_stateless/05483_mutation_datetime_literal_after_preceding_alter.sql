-- Tags: no-replicated-database
-- no-replicated-database: a `Replicated` database does not allow `ALTER` commands of different types in one query.

-- With `session_timezone`, a string literal compared with a `DateTime` column in a mutation gets the time zone of the session
-- explicitly (05459). The mutation is rewritten against the columns of the table when it is submitted, after the preceding
-- commands of the statement are committed, so it sees a column that the statement adds.

SET session_timezone = 'Asia/Tokyo';
SET mutations_sync = 2;

SELECT '--- an alias of a column that the statement adds';
DROP TABLE IF EXISTS t_preceding_alias;
CREATE TABLE t_preceding_alias (id UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_preceding_alias VALUES (1);
ALTER TABLE t_preceding_alias ADD COLUMN c UInt8, UPDATE c = 1 WHERE isNotNull(identity(c) AS v);
SELECT id, c FROM t_preceding_alias;

SELECT '--- a comparison with a DateTime column that the statement adds';
DROP TABLE IF EXISTS t_preceding_datetime;
CREATE TABLE t_preceding_datetime (id UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_preceding_datetime VALUES (1);
ALTER TABLE t_preceding_datetime ADD COLUMN event DateTime, DELETE WHERE event = '2020-01-01 00:00:00';
SELECT extract(command, 'event = [^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_preceding_datetime';

DROP TABLE t_preceding_alias;
DROP TABLE t_preceding_datetime;
