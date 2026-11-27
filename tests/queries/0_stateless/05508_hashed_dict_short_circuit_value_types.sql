-- dictGetOrDefault on a HASHED dictionary evaluates its default expression lazily:
-- the dictionary fills only the keys it holds and the default expression is computed
-- just for the keys that miss, so each attribute type below is retrieved through the
-- short-circuit branch of HashedDictionary::getColumn instead of a default column.
-- Expected behaviour:
--   docs/reference/functions/regular-functions/ext-dict-functions.mdx (dictGetOrDefault)
--   docs/concepts/features/dictionaries/index.mdx (hashed layout)
-- 'force_enable' makes every default expression lazily executed; the randomizer can set
-- this to 'disable', which routes the lookup through the default-column branch instead.
SET short_circuit_function_evaluation = 'force_enable';
SET short_circuit_function_evaluation_for_nulls_threshold = 1;

DROP TABLE IF EXISTS hashed_dict_source;
CREATE TABLE hashed_dict_source
(
    id UInt64,
    arr Array(String),
    m Map(String, UInt64),
    j JSON,
    nj Nullable(JSON)
)
ENGINE = Memory;

INSERT INTO hashed_dict_source VALUES (1, ['a', 'b'], map('x', 1), '{"k":1}', '{"k":1}'), (2, ['c'], map('y', 2), '{"k":2}', NULL);

DROP DICTIONARY IF EXISTS hashed_dict_sc;
CREATE DICTIONARY hashed_dict_sc
(
    id UInt64,
    arr Array(String),
    m Map(String, UInt64),
    j JSON,
    nj Nullable(JSON)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'hashed_dict_source' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

-- Keys 1 and 2 are held by the dictionary and return the stored value.
-- Keys 0 and 3 are absent and return the per-row value of the default expression.
SELECT 'array attribute';
SELECT number, dictGetOrDefault('hashed_dict_sc', 'arr', number, [toString(number)])
FROM numbers(4) ORDER BY number;

SELECT 'map attribute';
SELECT number, dictGetOrDefault('hashed_dict_sc', 'm', number, map(toString(number), number))
FROM numbers(4) ORDER BY number;

-- The JSON defaults are offset by 100, so a default can never be mistaken for a stored
-- value and the reference proves that the two found keys keep what the dictionary holds.
SELECT 'json attribute';
SELECT number, toString(dictGetOrDefault('hashed_dict_sc', 'j', number, CAST(concat('{"k":', toString(number + 100), '}'), 'JSON')))
FROM numbers(4) ORDER BY number;

-- The nullable attribute holds an explicit NULL for key 2: a stored NULL stays NULL,
-- only the absent keys 0 and 3 fall back to the default expression.
SELECT 'nullable json';
SELECT number, toString(dictGetOrDefault('hashed_dict_sc', 'nj', number, CAST(concat('{"k":', toString(number + 100), '}'), 'Nullable(JSON)')))
FROM numbers(4) ORDER BY number;

DROP DICTIONARY hashed_dict_sc;
DROP TABLE hashed_dict_source;
