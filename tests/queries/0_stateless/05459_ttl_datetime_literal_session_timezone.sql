-- The `TTL` of a table is evaluated by background merges without the session, so a `DateTime` string literal in it was
-- read in the server time zone instead of `session_timezone`, and `TTL ... DELETE WHERE event = '...'` removed other rows
-- than it named. The literal now gets the time zone of the session in the stored definition, as in the mutations. The
-- merges run without the session time zone here, like background merges.

SET session_timezone = 'Asia/Tokyo';

DROP TABLE IF EXISTS t_create;
DROP TABLE IF EXISTS t_modify;
DROP TABLE IF EXISTS t_source;
DROP TABLE IF EXISTS t_copy;
DROP TABLE IF EXISTS t_modify_column;
DROP TABLE IF EXISTS t_add_column;
DROP TABLE IF EXISTS t_clear_column;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_nested_to_string;
DROP TABLE IF EXISTS t_nested_modify;
DROP TABLE IF EXISTS t_tuple_create;
DROP TABLE IF EXISTS t_tuple_modify;
DROP TABLE IF EXISTS t_tuple_mutation;
DROP TABLE IF EXISTS t_untyped;
DROP TABLE IF EXISTS t_lambda_create;
DROP TABLE IF EXISTS t_lambda_modify;
DROP TABLE IF EXISTS t_lambda_element;
DROP TABLE IF EXISTS t_lambda_datetime;
DROP TABLE IF EXISTS t_lambda_mutation;
DROP TABLE IF EXISTS t_lambda_fixed;
DROP TABLE IF EXISTS t_lambda_tuple;
DROP TABLE IF EXISTS t_lambda_tuple_mutation;
DROP TABLE IF EXISTS t_alias_mutation;
DROP TABLE IF EXISTS t_alias_temporal;
DROP TABLE IF EXISTS t_alias_temporal_changed;
DROP TABLE IF EXISTS t_alias_lower_precision;
DROP TABLE IF EXISTS t_alias_nullable_tuple;
DROP TABLE IF EXISTS t_alias_prefer_column;
DROP TABLE IF EXISTS t_alias_prefer_column_memory;
DROP TABLE IF EXISTS t_alias_scoped_lambda;
DROP TABLE IF EXISTS t_alias_scoped_subquery;
DROP TABLE IF EXISTS t_alias_scalar_subquery;
DROP TABLE IF EXISTS t_rollup_create;
DROP TABLE IF EXISTS t_rollup_modify;

CREATE TABLE t_create (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00'
SETTINGS merge_with_ttl_timeout = 86400;
INSERT INTO t_create VALUES ('2020-01-01 00:00:00', 1), ('2020-01-01 09:00:00', 2);
OPTIMIZE TABLE t_create FINAL SETTINGS session_timezone = '';
SELECT 'CREATE', groupArray(id) FROM (SELECT id FROM t_create ORDER BY id);
SELECT extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_create';

CREATE TABLE t_modify (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id SETTINGS merge_with_ttl_timeout = 86400;
INSERT INTO t_modify VALUES ('2020-01-01 00:00:00', 1), ('2020-01-01 09:00:00', 2);
ALTER TABLE t_modify MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
OPTIMIZE TABLE t_modify FINAL SETTINGS session_timezone = '';
SELECT 'MODIFY TTL', groupArray(id) FROM (SELECT id FROM t_modify ORDER BY id);

-- A `TTL` that `CREATE TABLE ... AS` copies from the source table keeps the meaning it has there: here, a literal written
-- without the session time zone.
SET session_timezone = '';
CREATE TABLE t_source (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
SET session_timezone = 'Asia/Tokyo';
CREATE TABLE t_copy AS t_source;
SELECT 'AS', extract(engine_full, 'WHERE [^\']*\'[^\']*\'') FROM system.tables WHERE database = currentDatabase() AND name = 't_copy';

-- The type of the column is the one that the preceding commands of the `ALTER` give it: here, a literal of a `UTC` column.
CREATE TABLE t_modify_column (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id SETTINGS merge_with_ttl_timeout = 86400;
INSERT INTO t_modify_column VALUES (1577836800, 1), (1577804400, 2);
ALTER TABLE t_modify_column MODIFY COLUMN event DateTime('UTC'),
    MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
OPTIMIZE TABLE t_modify_column FINAL SETTINGS session_timezone = '';
SELECT 'MODIFY COLUMN, MODIFY TTL', groupArray(id) FROM (SELECT id FROM t_modify_column ORDER BY id);

CREATE TABLE t_add_column (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_add_column ADD COLUMN event DateTime, MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
SELECT 'ADD COLUMN, MODIFY TTL', extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_add_column';

-- `CLEAR COLUMN` keeps the column in the table.
CREATE TABLE t_clear_column (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_clear_column CLEAR COLUMN event, MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
SELECT 'CLEAR COLUMN, MODIFY TTL', extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_clear_column';

-- The preceding commands are applied as the `ALTER` applies them, also to the flattened columns of a `Nested` column, such
-- as `n.ts`: replacing `n` changes the type of `n.ts`, in both directions.
CREATE TABLE t_nested (event DateTime, id UInt8, n Nested(ts String)) ENGINE = MergeTree ORDER BY id SETTINGS merge_with_ttl_timeout = 86400;
ALTER TABLE t_nested DROP COLUMN n, ADD COLUMN n Nested(ts DateTime),
    MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists(x -> x = '2020-01-01 00:00:00', n.ts);
INSERT INTO t_nested VALUES (0, 1, [1577804400]), (0, 2, [1577836800]);
OPTIMIZE TABLE t_nested FINAL SETTINGS session_timezone = '';
SELECT 'Nested DROP, ADD, MODIFY TTL', groupArray(id) FROM (SELECT id FROM t_nested ORDER BY id);
CREATE TABLE t_nested_to_string (event DateTime, id UInt8, n Nested(ts DateTime)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_nested_to_string DROP COLUMN n, ADD COLUMN n Nested(ts String),
    MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists(x -> x = 'keep', n.ts);
SELECT 'Nested to String', extract(engine_full, 'WHERE .* SETTINGS') FROM system.tables WHERE database = currentDatabase() AND name = 't_nested_to_string';
CREATE TABLE t_nested_modify (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_nested_modify ADD COLUMN n Nested(ts DateTime), MODIFY COLUMN n.ts Array(String),
    MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists(x -> x = 'keep', n.ts);
SELECT 'Nested ADD, MODIFY COLUMN', extract(engine_full, 'WHERE .* SETTINGS') FROM system.tables WHERE database = currentDatabase() AND name = 't_nested_modify';

-- A subcolumn, such as the element `n.ts` of a tuple, in `CREATE`, in `MODIFY TTL` and in a mutation.
CREATE TABLE t_tuple_create (event DateTime, id UInt8, n Tuple(ts DateTime)) ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE n.ts = '2020-01-01 00:00:00' SETTINGS merge_with_ttl_timeout = 86400;
INSERT INTO t_tuple_create VALUES (0, 1, (1577804400)), (0, 2, (1577836800));
OPTIMIZE TABLE t_tuple_create FINAL SETTINGS session_timezone = '';
SELECT 'Tuple element, CREATE', groupArray(id) FROM (SELECT id FROM t_tuple_create ORDER BY id);
CREATE TABLE t_tuple_modify (event DateTime, id UInt8, n Tuple(ts DateTime)) ENGINE = MergeTree ORDER BY id SETTINGS merge_with_ttl_timeout = 86400;
ALTER TABLE t_tuple_modify MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE n.ts = '2020-01-01 00:00:00';
INSERT INTO t_tuple_modify VALUES (0, 1, (1577804400)), (0, 2, (1577836800));
OPTIMIZE TABLE t_tuple_modify FINAL SETTINGS session_timezone = '';
SELECT 'Tuple element, MODIFY TTL', groupArray(id) FROM (SELECT id FROM t_tuple_modify ORDER BY id);
CREATE TABLE t_tuple_mutation (id UInt8, n Tuple(ts DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_tuple_mutation VALUES (1, (1577804400)), (2, (1577836800));
ALTER TABLE t_tuple_mutation DELETE WHERE n.ts = '2020-01-01 00:00:00' SETTINGS mutations_sync = 2;
SELECT 'Tuple element, DELETE', groupArray(id) FROM (SELECT id FROM t_tuple_mutation ORDER BY id);

-- A command that the statement is rejected for is not applied before the validation.
CREATE TABLE t_untyped (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_untyped ADD COLUMN x DEFAULT 0, MODIFY TTL event + INTERVAL 1 DAY; -- { serverError BAD_ARGUMENTS }

-- The assignments of `TTL ... GROUP BY ... SET` get the time zone too. Only the stored definition is checked: a merge with such a
-- rule fails on master with `NOT_FOUND_COLUMN_IN_BLOCK` for the comparison inside the aggregate function.
CREATE TABLE t_rollup_create (k UInt8, event DateTime, value UInt64) ENGINE = MergeTree ORDER BY k
TTL event + INTERVAL 1 SECOND GROUP BY k SET value = sumIf(value, event = '2020-01-01 00:00:00'), event = max(event);
SELECT 'GROUP BY SET', position(engine_full, 'sumIf(value, event = toDateTime(\'2020-01-01 00:00:00\', \'Asia/Tokyo\'))') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_rollup_create';

CREATE TABLE t_rollup_modify (k UInt8, event DateTime, value UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_rollup_modify MODIFY TTL event + INTERVAL 1 SECOND GROUP BY k
    SET value = sumIf(value, event = '2020-01-01 00:00:00'), event = max(event);
SELECT 'MODIFY TTL GROUP BY SET', position(engine_full, 'sumIf(value, event = toDateTime(\'2020-01-01 00:00:00\', \'Asia/Tokyo\'))') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_rollup_modify';

-- A lambda parameter has the type of the element it is bound to and shadows the column of the same name: a `String` keeps the
-- string.
CREATE TABLE t_lambda_create (event DateTime, tags Array(String)) ENGINE = MergeTree ORDER BY tuple()
TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists(event -> event = 'keep', tags) AND event = '2020-01-01 00:00:00';
SELECT 'lambda', extract(engine_full, 'WHERE .*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_create';

CREATE TABLE t_lambda_modify (event DateTime, tags Array(String)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_lambda_modify MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists(event -> event = 'keep', tags) AND event = '2020-01-01 00:00:00';
SELECT 'MODIFY TTL lambda', extract(engine_full, 'WHERE .*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_modify';

-- An element of a lambda parameter is not the column with the same compound name either.
CREATE TABLE t_lambda_element (`n.event` DateTime, arr Array(Tuple(event String))) ENGINE = MergeTree ORDER BY tuple()
TTL `n.event` + INTERVAL 1 SECOND DELETE WHERE arrayExists(n -> n.event = 'keep', arr);
SELECT 'lambda element', extract(engine_full, 'WHERE .*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_element';

-- The mutations share the rewrite.
INSERT INTO t_lambda_modify SETTINGS session_timezone = '' VALUES ('2100-01-01 00:00:00', ['keep']), ('2100-01-01 00:00:00', ['other']);
ALTER TABLE t_lambda_modify DELETE WHERE arrayExists(event -> event = 'keep', tags) SETTINGS mutations_sync = 2;
SELECT 'DELETE lambda', tags FROM t_lambda_modify;

-- A lambda parameter of a `DateTime` type gets the time zone whatever its name, over an array, over a map, as the element of
-- `arrayFold` and over a parameter of an enclosing lambda. The type of a parameter bound to an expression is not known, and the
-- parameter is matched with the columns by its name.
CREATE TABLE t_lambda_datetime (event DateTime, arr Array(DateTime), m Map(String, DateTime), nested Array(Array(DateTime)))
ENGINE = MergeTree ORDER BY tuple()
TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists(event -> event = '2020-01-01 00:00:00', arr)
    OR arrayExists(x -> x = '2020-01-01 00:00:00', arr) OR mapExists((k, v) -> v = '2020-01-01 00:00:00', m)
    OR arrayFold((acc, x) -> acc OR x = '2020-01-01 00:00:00', arr, false)
    OR arrayExists(a -> arrayExists(b -> b = '2020-01-01 00:00:00', a), nested)
    OR arrayExists(event -> event = '2020-01-01 00:00:00', arrayMap(y -> y, arr));
SELECT 'lambda DateTime', extract(engine_full, 'WHERE .*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_datetime';

-- A function with a fixed argument between the lambda and the arrays binds the parameters to the arrays after it.
CREATE TABLE t_lambda_fixed (event DateTime, arr Array(DateTime), tags Array(String)) ENGINE = MergeTree ORDER BY tuple()
TTL event + INTERVAL 1 SECOND DELETE WHERE length(arrayPartialSort((x, y) -> y = 'keep', 1, arr, tags)) > 0
    OR length(arrayTopK((x, y) -> x = '2020-01-01 00:00:00', 1, arr, tags)) > 0;
SELECT 'lambda fixed argument', extract(engine_full, 'WHERE .*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_fixed';
ALTER TABLE t_lambda_fixed MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE length(arrayBottomK((x, y) -> y = 'keep', 1, arr, tags)) > 0;
SELECT 'MODIFY TTL lambda fixed argument', extract(engine_full, 'WHERE .*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_fixed';

CREATE TABLE t_lambda_mutation (id UInt8, event DateTime, arr Array(DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_mutation VALUES (1, 0, [1577804400]), (2, 0, [1577836800]);
SELECT 'SELECT lambda DateTime', id FROM t_lambda_mutation WHERE arrayExists(event -> event = '2020-01-01 00:00:00', arr);
ALTER TABLE t_lambda_mutation DELETE WHERE arrayExists(event -> event = '2020-01-01 00:00:00', arr) SETTINGS mutations_sync = 2;
SELECT 'DELETE lambda DateTime', id FROM t_lambda_mutation;

-- The parameters are bound as the function binds them, also unpacked from an array of tuples and after a fixed argument
-- of a function that is not named anywhere in the rewrite.
CREATE TABLE t_lambda_tuple (event DateTime, arr Array(Tuple(DateTime, String)), packed Array(Tuple(String, DateTime)))
ENGINE = MergeTree ORDER BY tuple()
TTL event + INTERVAL 1 SECOND DELETE WHERE arrayExists((event, tag) -> event = '2020-01-01 00:00:00' AND tag = 'keep', arr)
    OR arrayPackBitsToFixedString((tag, event) -> event = '2020-01-01 00:00:00', 1, packed) != unhex('00');
SELECT 'lambda tuple', extract(engine_full, 'WHERE .*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_lambda_tuple';

CREATE TABLE t_lambda_tuple_mutation (id UInt8, arr Array(Tuple(DateTime, String)), packed Array(Tuple(String, DateTime)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lambda_tuple_mutation VALUES
    (1, [(1577804400, 'keep')], []), (2, [(1577836800, 'keep')], []), (3, [], [('keep', 1577804400)]), (4, [], [('keep', 1577836800)]);
ALTER TABLE t_lambda_tuple_mutation DELETE WHERE arrayExists((event, tag) -> event = '2020-01-01 00:00:00', arr) SETTINGS mutations_sync = 2;
ALTER TABLE t_lambda_tuple_mutation DELETE WHERE arrayPackBitsToFixedString((tag, event) -> event = '2020-01-01 00:00:00', 1, packed) != unhex('00')
SETTINGS mutations_sync = 2;
SELECT 'DELETE lambda tuple', groupArray(id) FROM (SELECT id FROM t_lambda_tuple_mutation ORDER BY id);

-- An identifier that names an alias of the expression is the aliased expression, not the column with its name: the array
-- `c` of the lambda, and the `String` alias `event`, whose comparison keeps its string.
CREATE TABLE t_alias_mutation (id UInt8, c UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_mutation VALUES (1, 0, 0), (2, 0, 0), (3, 0, 0);
ALTER TABLE t_alias_mutation DELETE WHERE id = 1 AND has([1, 2] AS c, 1) AND arrayExists(x -> x = 1, c) SETTINGS mutations_sync = 2;
ALTER TABLE t_alias_mutation DELETE WHERE (toString(id) AS event) != '' AND event = '2' SETTINGS mutations_sync = 2;
SELECT 'DELETE alias', groupArray(id) FROM (SELECT id FROM t_alias_mutation ORDER BY id);

-- An alias of a `DateTime` expression is a `DateTime`, so its comparison gets the time zone, also when the alias has the name
-- of the column that its expression reads.
CREATE TABLE t_alias_temporal (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_temporal VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-01 09:00:00');
ALTER TABLE t_alias_temporal DELETE WHERE isNotNull(identity(event) AS event) AND event = '2020-01-01 00:00:00' SETTINGS mutations_sync = 2;
SELECT 'DELETE temporal alias', groupArray(id) FROM (SELECT id FROM t_alias_temporal ORDER BY id);
SELECT extract(command, 'event = [^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_temporal';

-- So is an alias that changes the nullability or the precision of the column with its name, which is a `DateTime` too.
CREATE TABLE t_alias_temporal_changed (id UInt8, event DateTime, n Nullable(DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_temporal_changed SELECT number + 1, toDateTime('2020-01-01 00:00:00') + intDiv(number, 2) * 86400 + number % 2 * 32400 AS event, event
FROM numbers(6);
ALTER TABLE t_alias_temporal_changed DELETE WHERE isNotNull(toNullable(event) AS event) AND event = '2020-01-01 00:00:00' SETTINGS mutations_sync = 2;
ALTER TABLE t_alias_temporal_changed DELETE WHERE isNotNull(assumeNotNull(n) AS n) AND n = '2020-01-02 00:00:00' SETTINGS mutations_sync = 2;
ALTER TABLE t_alias_temporal_changed DELETE WHERE isNotNull(toDateTime64(event, 3) AS event) AND event = '2020-01-03 00:00:00' SETTINGS mutations_sync = 2;
SELECT 'DELETE changed temporal alias', groupArray(id) FROM (SELECT id FROM t_alias_temporal_changed ORDER BY id);
SELECT extract(command, '(?:event|n) = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_temporal_changed' ORDER BY mutation_id;

-- A literal compared with an alias with a lower precision than the column with its name, or than its element, is converted
-- with the precision of the column: the mutation may run with `prefer_column_name_to_alias` in the profile of the server, where
-- the identifier names the column, and a conversion with the type of the alias would drop the fraction of a second that the
-- column compares. With the default settings, it names the alias and deletes the right rows.
CREATE TABLE t_alias_lower_precision (id UInt8, event DateTime64(3), n Tuple(ts DateTime64(3))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_lower_precision SELECT number + 1, toDateTime64('2020-01-01 00:00:00', 3) + number * 32400 AS event, tuple(event) FROM numbers(3);
ALTER TABLE t_alias_lower_precision DELETE WHERE isNotNull(toDateTime(event) AS event) AND event = '2020-01-01 00:00:00' SETTINGS mutations_sync = 2;
ALTER TABLE t_alias_lower_precision DELETE WHERE isNotNull(CAST(n, 'Tuple(ts DateTime)') AS n) AND n.ts = '2020-01-01 09:00:00' SETTINGS mutations_sync = 2;
SELECT 'DELETE lower precision alias', groupArray(id) FROM (SELECT id FROM t_alias_lower_precision ORDER BY id);
SELECT extract(command, '(?:event|n\\.ts) = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_lower_precision' ORDER BY mutation_id;

-- An element of a `Nullable` tuple, of an alias or of a lambda parameter over an array of them, is a `Nullable` `DateTime`, so
-- its comparison gets the time zone.
CREATE TABLE t_alias_nullable_tuple (id UInt8, n Tuple(ts DateTime)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_nullable_tuple VALUES (1, tuple('2020-01-01 00:00:00')), (2, tuple('2020-01-01 09:00:00')), (3, tuple('2020-01-01 18:00:00'));
ALTER TABLE t_alias_nullable_tuple DELETE WHERE isNotNull(toNullable(n) AS n) AND n.ts = '2020-01-01 00:00:00' SETTINGS mutations_sync = 2;
ALTER TABLE t_alias_nullable_tuple DELETE WHERE notEmpty([toNullable(n)] AS arr) AND arrayExists(x -> x.ts = '2020-01-01 09:00:00', arr) SETTINGS mutations_sync = 2;
SELECT 'DELETE Nullable tuple', groupArray(id) FROM (SELECT id FROM t_alias_nullable_tuple ORDER BY id);
SELECT extract(command, '[nx]\\.ts = .*') FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_nullable_tuple' ORDER BY mutation_id;

-- With `prefer_column_name_to_alias`, the identifier names the `DateTime` column rather than the `String` alias with its name,
-- but the mutation runs with the settings of the server, where it names the alias, so the comparison keeps its string and the
-- mutation does not fail (05485 checks an alias that is a `DateTime` too).
CREATE TABLE t_alias_prefer_column (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_prefer_column VALUES (1, '2020-01-01 00:00:00');
ALTER TABLE t_alias_prefer_column DELETE WHERE isNotNull(toString(event) AS event) AND event = '2020-01-01 00:00:00'
SETTINGS mutations_sync = 2, prefer_column_name_to_alias = 1;
SELECT extract(command, 'event = [^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_prefer_column';

-- A `Memory` table runs the mutation in the session, where `c` names the column with `prefer_column_name_to_alias`, so the
-- mutation is valid, also with the session time zone.
CREATE TABLE t_alias_prefer_column_memory (id UInt8, c UInt8) ENGINE = Memory;
INSERT INTO t_alias_prefer_column_memory VALUES (1, 0), (2, 5);
ALTER TABLE t_alias_prefer_column_memory DELETE WHERE has([1, 2] AS c, 1) AND c = 0 SETTINGS prefer_column_name_to_alias = 1;
SELECT 'DELETE prefer column Memory', groupArray(id) FROM (SELECT id FROM t_alias_prefer_column_memory ORDER BY id);

-- An alias inside a lambda or a subquery is visible only there, so the comparison outside gets the time zone of the column,
-- while one inside the lambda with the `String` alias keeps its string.
CREATE TABLE t_alias_scoped_lambda (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_scoped_lambda VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-01 09:00:00'), (3, '2020-01-01 00:00:00');
ALTER TABLE t_alias_scoped_lambda DELETE WHERE arrayExists(x -> (x AS event) = 1, [1]) AND event = '2020-01-01 00:00:00'
SETTINGS mutations_sync = 2;
SELECT 'DELETE lambda-scoped alias', groupArray(id) FROM (SELECT id FROM t_alias_scoped_lambda ORDER BY id);
ALTER TABLE t_alias_scoped_lambda DELETE WHERE arrayExists(x -> isNotNull(toString(x) AS event) AND event = '2020-01-01 00:00:00', [id])
SETTINGS mutations_sync = 2;
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_scoped_lambda' ORDER BY mutation_id;
CREATE TABLE t_alias_scoped_subquery (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_scoped_subquery VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-01 09:00:00'), (3, '2020-01-01 00:00:00');
ALTER TABLE t_alias_scoped_subquery DELETE WHERE id IN (SELECT 3 AS event) AND event = '2020-01-01 00:00:00' SETTINGS mutations_sync = 2;
SELECT 'DELETE subquery-scoped alias', groupArray(id) FROM (SELECT id FROM t_alias_scoped_subquery ORDER BY id);
-- The alias of a scalar subquery is seen outside it, so the identifier with its name is the `String` subquery, not the
-- `DateTime` column, and the comparison keeps its string.
CREATE TABLE t_alias_scalar_subquery (id UInt8, s DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_scalar_subquery VALUES (1, '2020-01-01 00:00:00');
ALTER TABLE t_alias_scalar_subquery DELETE WHERE ((SELECT 'abc') AS s) != '' AND s = '2020-01-01 00:00:00' SETTINGS mutations_sync = 2;
SELECT 'DELETE scalar subquery alias', groupArray(id) FROM (SELECT id FROM t_alias_scalar_subquery ORDER BY id);
SELECT extract(command, 's = [^)]*\\)') FROM system.mutations WHERE database = currentDatabase() AND table = 't_alias_scalar_subquery';

DROP TABLE t_create;
DROP TABLE t_modify;
DROP TABLE t_source;
DROP TABLE t_copy;
DROP TABLE t_modify_column;
DROP TABLE t_add_column;
DROP TABLE t_untyped;
DROP TABLE t_rollup_create;
DROP TABLE t_rollup_modify;
DROP TABLE t_clear_column;
DROP TABLE t_lambda_create;
DROP TABLE t_lambda_modify;
DROP TABLE t_lambda_element;
DROP TABLE t_lambda_datetime;
DROP TABLE t_lambda_mutation;
DROP TABLE t_lambda_fixed;
DROP TABLE t_lambda_tuple;
DROP TABLE t_lambda_tuple_mutation;
DROP TABLE t_alias_mutation;
DROP TABLE t_alias_temporal;
DROP TABLE t_alias_temporal_changed;
DROP TABLE t_alias_lower_precision;
DROP TABLE t_alias_nullable_tuple;
DROP TABLE t_alias_prefer_column;
DROP TABLE t_alias_prefer_column_memory;
DROP TABLE t_alias_scoped_lambda;
DROP TABLE t_alias_scoped_subquery;
DROP TABLE t_alias_scalar_subquery;
DROP TABLE t_nested;
DROP TABLE t_nested_to_string;
DROP TABLE t_nested_modify;
DROP TABLE t_tuple_create;
DROP TABLE t_tuple_modify;
DROP TABLE t_tuple_mutation;
