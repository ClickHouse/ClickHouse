-- HASHED_ARRAY dictionary attribute materialization for the container attribute
-- types. `dictGetOrDefault` with a lazily evaluated default expression takes the
-- short-circuit path, `dictGet` takes the attribute default-value path.
-- Behaviour reference: the `dictGet` / `dictGetOrDefault` descriptions in
-- docs/en/sql-reference/functions/ext-dict-functions.md and the `hashed_array`
-- layout description in docs/en/sql-reference/dictionaries/index.md.
--
-- Key 0 is present in the dictionary and key 9 is missing. The default
-- expression `intDiv(100, k)` divides by zero for key 0, so the
-- `dictGetOrDefault` queries return a result only when the default expression is
-- evaluated lazily, for the missing key alone.

SET short_circuit_function_evaluation = 'enable';
-- `short_circuit_function_evaluation_for_nulls_threshold` is randomized by
-- tests/clickhouse-test; pin it to its default so that the nullable attributes
-- below are materialized the same way in every run.
SET short_circuit_function_evaluation_for_nulls_threshold = 1;

DROP DICTIONARY IF EXISTS attribute_dict;
DROP TABLE IF EXISTS attribute_source;

CREATE TABLE attribute_source
(
    id UInt64,
    arr Array(UInt64),
    m Map(String, UInt64),
    j JSON,
    jn Nullable(JSON),
    n Nullable(UInt64)
)
ENGINE = Memory;

INSERT INTO attribute_source VALUES
    (0, [1, 2], map('a', 1), '{"a":1}', '{"a":1}', 10),
    (1, [3], map('b', 2), '{"b":2}', NULL, NULL),
    (2, [], map('c', 3, 'd', 4), '{"c":3}', '{"c":3}', 30);

CREATE DICTIONARY attribute_dict
(
    id UInt64,
    arr Array(UInt64),
    m Map(String, UInt64),
    j JSON,
    jn Nullable(JSON),
    n Nullable(UInt64)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'attribute_source'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED_ARRAY());

-- Default-value path: the missing key 9 takes the attribute's own default, an
-- empty array for `arr` and NULL for the nullable `n`.
SELECT 'dictGet Array';
SELECT k, dictGet('attribute_dict', 'arr', k)
FROM (SELECT arrayJoin([0, 1, 2, 9]) AS k) ORDER BY k;

SELECT 'dictGet Nullable(UInt64)';
SELECT k, dictGet('attribute_dict', 'n', k)
FROM (SELECT arrayJoin([0, 1, 2, 9]) AS k) ORDER BY k;

-- Short-circuit path: only the missing key 9 gets the default expression.
SELECT 'dictGetOrDefault Array';
SELECT k, dictGetOrDefault('attribute_dict', 'arr', k, [intDiv(100, k)])
FROM (SELECT arrayJoin([0, 1, 2, 9]) AS k) ORDER BY k;

SELECT 'dictGetOrDefault Map';
SELECT k, dictGetOrDefault('attribute_dict', 'm', k, map('z', intDiv(100, k)))
FROM (SELECT arrayJoin([0, 1, 2, 9]) AS k) ORDER BY k;

SELECT 'dictGetOrDefault JSON';
SELECT k, dictGetOrDefault('attribute_dict', 'j', k, CAST(concat('{"z":', toString(intDiv(100, k)), '}'), 'JSON'))
FROM (SELECT arrayJoin([0, 1, 2, 9]) AS k) ORDER BY k;

SELECT 'dictGetOrDefault Nullable(JSON)';
SELECT k, dictGetOrDefault('attribute_dict', 'jn', k, CAST(concat('{"z":', toString(intDiv(100, k)), '}'), 'Nullable(JSON)'))
FROM (SELECT arrayJoin([0, 1, 2, 9]) AS k) ORDER BY k;

DROP DICTIONARY attribute_dict;
DROP TABLE attribute_source;
