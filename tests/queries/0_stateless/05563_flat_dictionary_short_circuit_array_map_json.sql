-- dictGetOrDefault with a lazily executed (short-circuit) default expression on a FLAT
-- dictionary that has Array, Map, JSON and Nullable(JSON) attributes.
--
-- Behaviour source: docs/en/sql-reference/functions/ext-dict-functions.md (dictGetOrDefault)
-- and the description of `short_circuit_function_evaluation` in
-- docs/en/operations/settings/settings.md: a non-constant default expression is evaluated
-- only for the keys that the dictionary does not contain.
--
-- Existing dictionary tests pass a constant default for the complex attribute types, which
-- makes the default column eager; only String/UInt64 attributes were ever queried with a
-- lazily executed default.

SET short_circuit_function_evaluation = 'enable';

DROP DICTIONARY IF EXISTS flat_dict_complex;
DROP TABLE IF EXISTS dict_source_complex;

CREATE TABLE dict_source_complex
(
    id UInt64,
    arr Array(UInt64),
    m Map(String, UInt64),
    j JSON,
    jn Nullable(JSON)
) ENGINE = Memory;

INSERT INTO dict_source_complex VALUES (1, [10, 11], map('a', 1), '{"x":1}', '{"y":1}'), (2, [20], map('a', 2), '{"x":2}', NULL);

CREATE DICTIONARY flat_dict_complex
(
    id UInt64,
    arr Array(UInt64),
    m Map(String, UInt64),
    j JSON,
    jn Nullable(JSON)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'dict_source_complex'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

-- Every key is present. The default expression divides by zero on the second row, so the
-- query can only return rows if the default was never evaluated: the values below are the
-- stored ones and the default expression stayed lazy.
SELECT 'array hit', id, dictGetOrDefault('flat_dict_complex', 'arr', id, [intDiv(1, id - 2)])
FROM dict_source_complex ORDER BY id;

SELECT 'map hit', id, dictGetOrDefault('flat_dict_complex', 'm', id, map('z', intDiv(1, id - 2)))
FROM dict_source_complex ORDER BY id;

SELECT 'json hit', id,
       toString(dictGetOrDefault('flat_dict_complex', 'j', id, CAST(concat('{"d":', toString(intDiv(1, id - 2)), '}'), 'JSON')))
FROM dict_source_complex ORDER BY id;

-- The nullable attribute keeps the stored NULL of the second row, so the second row prints
-- \N and isNull() is 1.
SELECT 'json nullable hit', id,
       toString(dictGetOrDefault('flat_dict_complex', 'jn', id, CAST(concat('{"d":', toString(intDiv(1, id - 2)), '}'), 'JSON'))) AS v,
       isNull(v)
FROM dict_source_complex ORDER BY id;

-- No key is present: the lazy default expression is evaluated for every row and its
-- row-dependent value is returned.
SELECT 'array miss', id, dictGetOrDefault('flat_dict_complex', 'arr', id + 10, [intDiv(100, id)])
FROM dict_source_complex ORDER BY id;

SELECT 'map miss', id, dictGetOrDefault('flat_dict_complex', 'm', id + 10, map('z', intDiv(100, id)))
FROM dict_source_complex ORDER BY id;

SELECT 'json miss', id,
       toString(dictGetOrDefault('flat_dict_complex', 'j', id + 10, CAST(concat('{"d":', toString(intDiv(100, id)), '}'), 'JSON')))
FROM dict_source_complex ORDER BY id;

SELECT 'json nullable miss', id,
       toString(dictGetOrDefault('flat_dict_complex', 'jn', id + 10, CAST(concat('{"d":', toString(intDiv(100, id)), '}'), 'JSON'))) AS v,
       isNull(v)
FROM dict_source_complex ORDER BY id;

-- Contrast: with the lazy evaluation turned off the same default expression is evaluated
-- for every row, including the keys that are present, and the division by zero is raised.
SELECT 'array hit', id, dictGetOrDefault('flat_dict_complex', 'arr', id, [intDiv(1, id - 2)])
FROM dict_source_complex ORDER BY id
SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_DIVISION }

DROP DICTIONARY flat_dict_complex;
DROP TABLE dict_source_complex;
