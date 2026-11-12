-- With `session_timezone`, a string literal compared with an alias inside a lambda in a mutation gets the time zone of the session
-- explicitly if the alias is a `DateTime` without a time zone, as outside the lambda (05459), and keeps its string if the alias
-- is a `String`. The background mutation of `MergeTree` runs with the time zone of the server, so the stored command is checked.

SET session_timezone = 'Asia/Tokyo';
SET mutations_sync = 2;

SELECT '--- a DateTime alias inside a lambda';
DROP TABLE IF EXISTS t_lambda_body_alias;
CREATE TABLE t_lambda_body_alias (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_body_alias VALUES (1, 1577804400), (2, 1577836800);
SELECT id FROM t_lambda_body_alias WHERE arrayExists(x -> isNotNull(identity(event) AS event) AND event = '2020-01-01 00:00:00', [1]);
ALTER TABLE t_lambda_body_alias DELETE WHERE arrayExists(x -> isNotNull(identity(event) AS event) AND event = '2020-01-01 00:00:00', [1]);
SELECT extract(command, 'event = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_lambda_body_alias';
SELECT id FROM t_lambda_body_alias ORDER BY id;

-- An alias whose expression uses something else than the columns of the table, such as the alias of a lambda, has no known
-- type, and the mutation is accepted; the comparison with the column gets the time zone.
SELECT '--- an alias inside a lambda that invokes the alias of a lambda';
DROP TABLE IF EXISTS t_lambda_body_alias_invoke;
CREATE TABLE t_lambda_body_alias_invoke (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_body_alias_invoke VALUES (1, 1577804400), (2, 1577836800);
ALTER TABLE t_lambda_body_alias_invoke DELETE WHERE arrayExists(x -> (arrayExists(f, [1]) AS z) AND event = '2020-01-01 00:00:00', [1])
    AND arrayExists((y -> y = 1) AS f, [1]);
SELECT extract(command, 'event = toDateTime\\([^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_lambda_body_alias_invoke';
SELECT id FROM t_lambda_body_alias_invoke ORDER BY id;

SELECT '--- an alias inside a lambda that calls the alias of a lambda';
DROP TABLE IF EXISTS t_lambda_body_alias_call;
CREATE TABLE t_lambda_body_alias_call (id UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_body_alias_call VALUES (1), (2);
ALTER TABLE t_lambda_body_alias_call DELETE WHERE arrayExists((y -> y = 1) AS f, [1]) AND arrayExists(x -> (f(id) AS z), [1]);
SELECT id FROM t_lambda_body_alias_call ORDER BY id;

SELECT '--- a String alias inside a lambda';
DROP TABLE IF EXISTS t_lambda_body_alias_string;
CREATE TABLE t_lambda_body_alias_string (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_body_alias_string VALUES (1, 1577804400);
ALTER TABLE t_lambda_body_alias_string DELETE WHERE arrayExists(x -> isNotNull(toString(event) AS event) AND event = 'keep', [1]);
SELECT extract(command, 'event = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_lambda_body_alias_string';
SELECT id FROM t_lambda_body_alias_string ORDER BY id;

DROP TABLE t_lambda_body_alias;
DROP TABLE t_lambda_body_alias_string;
DROP TABLE t_lambda_body_alias_invoke;
DROP TABLE t_lambda_body_alias_call;
