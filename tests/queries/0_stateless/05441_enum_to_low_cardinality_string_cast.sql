-- Casting `Enum` to `LowCardinality(String)` gives the same names and errors as casting to `String`.

DROP TABLE IF EXISTS t_enum;
CREATE TABLE t_enum
(
    e8 Enum8('neg' = -128, 'zero' = 0, 'pos' = 127),
    e16 Enum16('min' = -32768, 'a' = -1, 'b' = 1000, 'max' = 32767),
    one Enum8('only' = 5)
) ENGINE = Memory;
INSERT INTO t_enum VALUES ('neg', 'min', 'only'), ('zero', 'a', 'only'), ('pos', 'b', 'only'), ('neg', 'max', 'only');

SELECT CAST(e8 AS LowCardinality(String)) AS s8, CAST(e16 AS LowCardinality(String)) AS s16, CAST(one AS LowCardinality(String)) AS s1, toTypeName(s8)
FROM t_enum;

DROP TABLE t_enum;

-- Blocks with more rows than enum values and with fewer rows than enum values.
SELECT count(), countIf(CAST(e AS LowCardinality(String)) = CAST(e AS String))
FROM (SELECT CAST(['a', 'b', 'c', 'd'][number % 4 + 1] AS Enum16('a' = -1000, 'b' = 0, 'c' = 1000, 'd' = 2000)) AS e FROM numbers(1000));

SELECT count(), countIf(CAST(e AS LowCardinality(String)) = CAST(e AS String)), arraySort(groupUniqArray(CAST(e AS LowCardinality(String))))
FROM (SELECT CAST(99 - number % 3, assumeNotNull((SELECT 'Enum8(' || arrayStringConcat(arrayMap(i -> '''v' || toString(i) || ''' = ' || toString(i - 100), range(200)), ', ') || ')'))) AS e FROM numbers(1000))
SETTINGS max_block_size = 100;

-- An empty enum name matches the default value of the dictionary.
SELECT count(), arraySort(groupUniqArray(CAST(e AS LowCardinality(String))))
FROM (SELECT CAST(['', 'a'][number % 2 + 1] AS Enum8('' = 1, 'a' = 2)) AS e FROM numbers(100));
SELECT CAST(CAST('' AS Enum8('' = 1, 'a' = 2)) AS LowCardinality(String)) AS s, s = '';

SELECT CAST(CAST(map('a', 1, 'b', 2), 'Map(Enum16(\'a\' = -5, \'b\' = 300), Int64)') AS Map(LowCardinality(String), Int64)) AS m, toTypeName(m);
SELECT CAST(CAST(['x', 'y', 'x'], 'Array(Enum8(\'x\' = -1, \'y\' = 1))') AS Array(LowCardinality(String))) AS a, toTypeName(a);
SELECT CAST(CAST([], 'Array(Enum8(\'x\' = 1))') AS Array(LowCardinality(String))) AS a, toTypeName(a);

-- NULL rows of a `Nullable(Tuple(...))` hold placeholder values that are not in the enum.
SELECT CAST(t AS Nullable(Tuple(LowCardinality(String)))) FROM format(RowBinary, 't Nullable(Tuple(Enum8(\'a\' = 1)))', '\x01');
SELECT CAST(t AS Nullable(Tuple(LowCardinality(String)))) FROM format(RowBinary, 't Nullable(Tuple(Enum16(\'a\' = 1)))', '\x01');

-- The result dictionary holds only the used names, so functions do not see the unused name '!'.
SELECT hex(base58Decode(CAST(e AS LowCardinality(String)))) FROM format(RowBinary, 'e Enum8(\'2\' = 1, \'!\' = 2)', '\x01');

-- `RowBinary` does not validate enum values.
SELECT CAST(e AS String) FROM format(RowBinary, 'e Enum8(\'a\' = 1)', '\x05'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(e AS LowCardinality(String)) FROM format(RowBinary, 'e Enum8(\'a\' = 1)', '\x05'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(e AS LowCardinality(String)) FROM format(RowBinary, 'e Enum16(\'a\' = 1, \'b\' = 3)', '\x02\x00'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
