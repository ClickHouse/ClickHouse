-- CAST of a Nullable(Tuple) does not convert the elements of its NULL rows.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mt;

CREATE TABLE src (k UInt8, x Nullable(Tuple(s String)), y Nullable(Tuple(s Nullable(String)))) ENGINE = Memory;
INSERT INTO src VALUES (1, NULL, NULL), (2, ('5'), ('5'));

SELECT k, CAST(x, 'Nullable(Tuple(s Int32))') FROM src ORDER BY k;
SELECT k, CAST(y, 'Nullable(Tuple(s String))') FROM src ORDER BY k;
SELECT k, x::Nullable(Tuple(s Decimal(9, 2))) FROM src ORDER BY k;

SELECT CAST(arrayElementOrNull([tuple(materialize('2020-01-01'))], number % 2 + 1), 'Nullable(Tuple(Date))') FROM numbers(3);
SELECT CAST(arrayElementOrNull([tuple(toLowCardinality(toNullable(materialize('a'))))], number % 2 + 1), 'Nullable(Tuple(String))') FROM numbers(3);
SELECT CAST(tuple(arrayElementOrNull([tuple(materialize('5'))], number % 2 + 1)), 'Tuple(Nullable(Tuple(Int32)))') FROM numbers(3);
SELECT CAST([arrayElementOrNull([tuple(materialize('5'))], number % 2 + 1)], 'Array(Nullable(Tuple(Int32)))') FROM numbers(3);
SELECT CAST(arrayElementOrNull([tuple(materialize(CAST('a', 'Enum8(\'a\' = 1)')))], number % 2 + 1), 'Nullable(String)') FROM numbers(3);
SELECT accurateCastOrNull(CAST(arrayElementOrNull([tuple(materialize('a'::Enum8('a' = 1)))], 2) AS Nullable(Tuple(Nullable(Enum8('a' = 1))))), 'Nullable(Tuple(LowCardinality(String)))');
SELECT accurateCastOrNull(arrayElementOrNull([tuple(materialize('a'::Enum8('a' = 1)))], number % 2 + 1), 'Nullable(String)') FROM numbers(3);
SELECT accurateCastOrNull(arrayElementOrNull([tuple(materialize('5')), tuple(materialize('x'))], number % 3 + 1), 'Nullable(Tuple(Int32))') FROM numbers(3);

CREATE TABLE dst (k UInt8, x Nullable(Tuple(s Int32))) ENGINE = Memory;
INSERT INTO dst SELECT k, x FROM src;
SELECT * FROM dst ORDER BY k;

CREATE TABLE mt (k UInt8, x Nullable(Tuple(s String))) ENGINE = MergeTree ORDER BY k;
INSERT INTO mt SELECT k, x FROM src;
ALTER TABLE mt MODIFY COLUMN x Nullable(Tuple(s Int32)) SETTINGS mutations_sync = 2;
SELECT * FROM mt ORDER BY k;

-- A NULL element under a non-NULL row and an unparsable value still throw.
SELECT CAST(tuple(CAST(NULL, 'Nullable(String)')), 'Nullable(Tuple(String))'); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT CAST(arrayElementOrNull([tuple(materialize('x'))], number % 2 + 1), 'Nullable(Tuple(Int32))') FROM numbers(3); -- { serverError CANNOT_PARSE_TEXT }

DROP TABLE src;
DROP TABLE dst;
DROP TABLE mt;
