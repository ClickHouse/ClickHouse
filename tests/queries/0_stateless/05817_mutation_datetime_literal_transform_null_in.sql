-- With `transform_null_in = 1`, the analyzer resolves `IN` and `NOT IN` to `nullIn` and `notNullIn`. A string literal in the tuple
-- of such an `IN` with a `DateTime` column on the left is read with `session_timezone`, as with `transform_null_in = 0`, so the
-- background mutation deletes the rows that the condition selects in the session.

SET session_timezone = 'Asia/Tokyo';
SET transform_null_in = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_null_in;

-- 1577804400 is 2020-01-01 00:00:00 in Asia/Tokyo, and 1577836800 is 2020-01-01 00:00:00 in UTC.
SELECT '--- IN';
CREATE TABLE t_null_in (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_null_in VALUES (1, 1577804400), (2, 1577836800);
ALTER TABLE t_null_in DELETE WHERE event IN tuple('2020-01-01 00:00:00', toString('2000-01-01 00:00:00'));
SELECT extract(command, 'event IN .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_null_in';
SELECT id FROM t_null_in ORDER BY id;
DROP TABLE t_null_in;

SELECT '--- NOT IN';
CREATE TABLE t_null_in (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_null_in VALUES (1, 1577804400), (2, 1577836800);
ALTER TABLE t_null_in DELETE WHERE event NOT IN tuple('2020-01-01 00:00:00', toString('2000-01-01 00:00:00'));
SELECT extract(command, 'event NOT IN .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_null_in';
SELECT id FROM t_null_in ORDER BY id;
DROP TABLE t_null_in;
