-- With `session_timezone`, a string literal compared with an identifier of a `DateTime` type without a time zone in a mutation
-- or a `TTL` gets the time zone of the session explicitly (05459). The type is the one of what the analyzer binds the identifier
-- to in its scope, so names in other scopes, aliases of lambdas and the arguments of higher-order functions are bound as in the
-- query. The background mutation of `MergeTree` runs with the time zone of the server, so the stored command is checked too.

SET session_timezone = 'Asia/Tokyo';
SET mutations_sync = 2;

-- The alias `identity` of a lambda in another lambda does not change what `identity` is in the first one.
SELECT '--- a lambda alias in another scope named like a function';
DROP TABLE IF EXISTS t_scoped_lambda_alias;
CREATE TABLE t_scoped_lambda_alias (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_scoped_lambda_alias VALUES (1, 1577804400), (2, 1577836800);
SELECT id FROM t_scoped_lambda_alias WHERE
    arrayExists(x -> isNotNull(identity(event) AS event) AND event = '2020-01-01 00:00:00', [1])
    AND arrayExists(y -> arrayExists((z -> z = 1) AS identity, [1]), [1]);
ALTER TABLE t_scoped_lambda_alias DELETE WHERE
    arrayExists(x -> isNotNull(identity(event) AS event) AND event = '2020-01-01 00:00:00', [1])
    AND arrayExists(y -> arrayExists((z -> z = 1) AS identity, [1]), [1]);
SELECT extract(command, 'event = toDateTime\\([^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_scoped_lambda_alias';
SELECT id FROM t_scoped_lambda_alias ORDER BY id;

SELECT '--- an alias of the alias of a lambda';
DROP TABLE IF EXISTS t_forwarded_lambda_alias;
CREATE TABLE t_forwarded_lambda_alias (id UInt8, arr Array(DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_forwarded_lambda_alias VALUES (1, [1577804400]), (2, [1577836800]);
ALTER TABLE t_forwarded_lambda_alias DELETE WHERE arrayExists((y -> y = 1) AS f, [1]) AND arrayExists(f AS g, [id]);
SELECT id FROM t_forwarded_lambda_alias ORDER BY id;
ALTER TABLE t_forwarded_lambda_alias DELETE WHERE arrayExists((x -> x = '2020-01-01 00:00:00') AS f, [toDateTime(0)]) OR arrayExists(f AS g, arr);
SELECT id FROM t_forwarded_lambda_alias ORDER BY id;

-- The parameter of a lambda over an expression has the type of its elements: a `String` parameter named like a `DateTime`
-- column keeps its string, and a `DateTime` parameter gets the time zone.
SELECT '--- a lambda over an expression';
DROP TABLE IF EXISTS t_lambda_over_expression;
CREATE TABLE t_lambda_over_expression (id UInt8, event DateTime, arr Array(DateTime), tags Array(String)) ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists(event -> event = 'keep', arrayMap(t -> t, tags));
SELECT extract(engine_full, 'WHERE .*\\)\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_over_expression';
INSERT INTO t_lambda_over_expression SETTINGS session_timezone = '' VALUES (1, '2100-01-01 00:00:00', [1577804400], []), (2, '2100-01-01 00:00:00', [1577836800], []);
ALTER TABLE t_lambda_over_expression DELETE WHERE arrayExists(x -> x = '2020-01-01 00:00:00', arrayMap(y -> y, arr));
SELECT id FROM t_lambda_over_expression ORDER BY id;

-- A lambda that is invoked through its alias with arrays of the same type gets the time zone in its shared body.
SELECT '--- a lambda alias invoked with arrays of the same type';
DROP TABLE IF EXISTS t_lambda_alias_same_type;
CREATE TABLE t_lambda_alias_same_type (id UInt8, a Array(DateTime), b Array(DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_alias_same_type VALUES (1, [1577804400], []), (2, [1577836800], []), (3, [], [1577804400]), (4, [], [1577836800]);
ALTER TABLE t_lambda_alias_same_type DELETE WHERE arrayExists((x -> x = '2020-01-01 00:00:00') AS f, a) OR arrayExists(f, b);
SELECT id FROM t_lambda_alias_same_type ORDER BY id;

-- A string literal that is also used otherwise through its alias keeps its string there.
SELECT '--- an aliased literal';
DROP TABLE IF EXISTS t_aliased_literal;
CREATE TABLE t_aliased_literal (id UInt8, event DateTime, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aliased_literal VALUES (1, 1577804400, '2020-01-01 00:00:00'), (2, 1577836800, '2020-01-01 00:00:00'), (3, 1577804400, 'other');
ALTER TABLE t_aliased_literal DELETE WHERE event = ('2020-01-01 00:00:00' AS lit) AND s = lit;
SELECT id FROM t_aliased_literal ORDER BY id;

-- The analyzer rewrites `IN` with an element that is not a constant into `has` of an array of the elements cast to the type of
-- the identifier, so the literal is read as that type there.
SELECT '--- IN with an element that is not a constant';
DROP TABLE IF EXISTS t_in_not_constant;
CREATE TABLE t_in_not_constant (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_in_not_constant VALUES (1, 1577804400), (2, 1577836800);
ALTER TABLE t_in_not_constant DELETE WHERE event IN tuple('2020-01-01 00:00:00', toDateTime(id));
SELECT id FROM t_in_not_constant ORDER BY id;

-- As with `=`, a literal in `IN` with an expression on the left that is not an identifier keeps its string.
SELECT '--- IN with an element that is not a constant and an expression on the left';
DROP TABLE IF EXISTS t_in_not_constant_expression;
CREATE TABLE t_in_not_constant_expression (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_in_not_constant_expression DELETE WHERE toStartOfDay(event) IN tuple('2020-01-01 00:00:00', toDateTime(id));
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_in_not_constant_expression';

-- A comparison in a subquery is evaluated by the background mutation too.
SELECT '--- a comparison in a subquery';
DROP TABLE IF EXISTS t_subquery;
DROP TABLE IF EXISTS t_subquery_events;
CREATE TABLE t_subquery (id UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_subquery_events (id UInt8, ts DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_subquery VALUES (1), (2);
INSERT INTO t_subquery_events VALUES (1, 1577804400), (2, 1577836800);
ALTER TABLE t_subquery DELETE WHERE id IN (SELECT id FROM t_subquery_events WHERE ts = '2020-01-01 00:00:00');
SELECT id FROM t_subquery ORDER BY id;

-- With `prefer_column_name_to_alias` in the mutation, `c` would name the `UInt8` column, and `has` would fail, so only the
-- binding with the settings of this query matters: the mutation is accepted, and the comparison gets the time zone.
SELECT '--- an alias that does not resolve with the other value of prefer_column_name_to_alias';
DROP TABLE IF EXISTS t_alias_other_setting;
CREATE TABLE t_alias_other_setting (id UInt8, event DateTime, c UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_other_setting VALUES (1, 1577804400, 0), (2, 1577836800, 0);
ALTER TABLE t_alias_other_setting DELETE WHERE has([1] AS c, 1) AND event = '2020-01-01 00:00:00' SETTINGS prefer_column_name_to_alias = 0;
SELECT id FROM t_alias_other_setting ORDER BY id;

DROP TABLE t_in_not_constant;
DROP TABLE t_in_not_constant_expression;
DROP TABLE t_subquery;
DROP TABLE t_subquery_events;
DROP TABLE t_alias_other_setting;
DROP TABLE t_scoped_lambda_alias;
DROP TABLE t_forwarded_lambda_alias;
DROP TABLE t_lambda_over_expression;
DROP TABLE t_lambda_alias_same_type;
DROP TABLE t_aliased_literal;
