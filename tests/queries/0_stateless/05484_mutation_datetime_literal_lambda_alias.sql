-- With `session_timezone`, a string literal compared with a lambda parameter of a `DateTime` type in a mutation gets the time
-- zone of the session explicitly (05459). The body of a lambda with an alias that the expression invokes is shared by the
-- invocations, which may bind its parameters to other types, so it is not rewritten with the type of one of them, nor with the
-- type of the column with the name of a parameter: the comparison through the alias with an array of `DateTime('UTC')` or of
-- `String` keeps its string. The first rows are left out of the results, since the comparison with the zoneless array, which
-- keeps its string too, depends on the time zone of the server.

SET session_timezone = 'Asia/Tokyo';
SET mutations_sync = 2;

SELECT '--- an explicit time zone';
DROP TABLE IF EXISTS t_lambda_alias_utc;
CREATE TABLE t_lambda_alias_utc (id UInt8, arr Array(DateTime), utc Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_alias_utc VALUES (1, [1577804400], []), (2, [], [1577836800]), (3, [], [1577804400]);
ALTER TABLE t_lambda_alias_utc DELETE WHERE arrayExists((x -> x = '2020-01-01 00:00:00') AS f, arr) OR arrayExists(f, utc);
SELECT id FROM t_lambda_alias_utc WHERE id != 1 ORDER BY id;

SELECT '--- a String';
DROP TABLE IF EXISTS t_lambda_alias_string;
CREATE TABLE t_lambda_alias_string (id UInt8, arr Array(DateTime), tags Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_alias_string VALUES (1, [1577804400], []), (2, [], ['2020-01-01 00:00:00']), (3, [], ['keep']);
ALTER TABLE t_lambda_alias_string DELETE WHERE arrayExists((x -> x = '2020-01-01 00:00:00') AS f, arr) OR arrayExists(f, tags);
SELECT id FROM t_lambda_alias_string WHERE id != 1 ORDER BY id;

SELECT '--- a parameter named like a DateTime column, and the parameter of a lambda inside the shared body';
DROP TABLE IF EXISTS t_lambda_alias_column;
CREATE TABLE t_lambda_alias_column
(
    id UInt8, event DateTime, arr Array(DateTime), utc Array(DateTime('UTC')),
    nested Array(Array(DateTime)), nested_utc Array(Array(DateTime('UTC')))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_alias_column VALUES
    (1, 0, [1577804400], [], [[1577804400]], []),
    (2, 0, [], [1577836800], [], []),
    (3, 0, [], [1577804400], [], []),
    (4, 0, [], [], [], [[1577836800]]),
    (5, 0, [], [], [], [[1577804400]]);
ALTER TABLE t_lambda_alias_column DELETE WHERE arrayExists((event -> event = '2020-01-01 00:00:00') AS f, arr) OR arrayExists(f, utc);
ALTER TABLE t_lambda_alias_column DELETE WHERE arrayExists((a -> arrayExists(event -> event = '2020-01-01 00:00:00', a)) AS g, nested)
    OR arrayExists(g, nested_utc);
SELECT id FROM t_lambda_alias_column WHERE id != 1 ORDER BY id;

-- A lambda with an alias that nothing invokes is not shared, so its parameter gets the time zone as without the alias.
SELECT '--- an alias that is not invoked';
DROP TABLE IF EXISTS t_lambda_alias_unused;
CREATE TABLE t_lambda_alias_unused (id UInt8, arr Array(DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_alias_unused VALUES (1, [1577804400]), (2, [1577836800]);
ALTER TABLE t_lambda_alias_unused DELETE WHERE arrayExists((x -> x = '2020-01-01 00:00:00') AS f, arr);
SELECT extract(command, 'x = [^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_lambda_alias_unused';
SELECT id FROM t_lambda_alias_unused ORDER BY id;

DROP TABLE t_lambda_alias_utc;
DROP TABLE t_lambda_alias_string;
DROP TABLE t_lambda_alias_column;
DROP TABLE t_lambda_alias_unused;
