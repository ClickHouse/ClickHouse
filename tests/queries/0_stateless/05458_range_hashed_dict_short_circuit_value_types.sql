-- dictGetOrDefault on a range_hashed dictionary evaluates its default expression lazily:
-- the dictionary fills the keys whose range matches and the default expression is computed
-- only for the keys that miss. Expected behaviour:
--   docs/reference/functions/regular-functions/ext-dict-functions.mdx (dictGetOrDefault)
--   docs/concepts/features/dictionaries/index.mdx (range_hashed layout)
-- 'force_enable' makes every default expression lazily executed, so each attribute type
-- below goes through the short-circuit retrieval path instead of the default-column one.
SET short_circuit_function_evaluation = 'force_enable';
SET short_circuit_function_evaluation_for_nulls_threshold = 1;

DROP TABLE IF EXISTS dict_src;
CREATE TABLE dict_src
(
    id UInt64,
    range_start Date,
    range_end Date,
    arr Array(String),
    m Map(String, UInt64),
    str String,
    nstr Nullable(String),
    n UInt64,
    j JSON,
    nj Nullable(JSON)
)
ENGINE = Memory;

INSERT INTO dict_src VALUES (1, '2024-01-01', '2024-01-31', ['a', 'b'], map('x', 1), 'one', 'none', 11, '{"k":1}', '{"k":1}'), (2, '2024-01-01', '2024-01-31', ['c'], map('y', 2), 'two', NULL, 22, '{"k":2}', NULL);

DROP DICTIONARY IF EXISTS range_dict_sc;
CREATE DICTIONARY range_dict_sc
(
    id UInt64,
    range_start Date,
    range_end Date,
    arr Array(String),
    m Map(String, UInt64),
    str String,
    nstr Nullable(String),
    n UInt64,
    j JSON,
    nj Nullable(JSON)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(RANGE_HASHED())
RANGE(MIN range_start MAX range_end);

-- Keys 1 and 2 are inside the stored range and return the stored value.
-- Keys 0 and 3 are absent and return the per-row value of the default expression.
SELECT 'Array(String)';
SELECT number, dictGetOrDefault('range_dict_sc', 'arr', number, toDate('2024-01-15'), [toString(number)])
FROM numbers(4) ORDER BY number;

SELECT 'Map(String, UInt64)';
SELECT number, dictGetOrDefault('range_dict_sc', 'm', number, toDate('2024-01-15'), map(toString(number), number))
FROM numbers(4) ORDER BY number;

SELECT 'String';
SELECT number, dictGetOrDefault('range_dict_sc', 'str', number, toDate('2024-01-15'), concat('d', toString(number)))
FROM numbers(4) ORDER BY number;

SELECT 'Nullable(String)';
SELECT number, dictGetOrDefault('range_dict_sc', 'nstr', number, toDate('2024-01-15'), concat('d', toString(number)))
FROM numbers(4) ORDER BY number;

SELECT 'UInt64';
SELECT number, dictGetOrDefault('range_dict_sc', 'n', number, toDate('2024-01-15'), number + 100)
FROM numbers(4) ORDER BY number;

SELECT 'JSON';
SELECT number, toString(dictGetOrDefault('range_dict_sc', 'j', number, toDate('2024-01-15'), CAST(concat('{"k":', toString(number), '}'), 'JSON')))
FROM numbers(4) ORDER BY number;

SELECT 'Nullable(JSON)';
SELECT number, toString(dictGetOrDefault('range_dict_sc', 'nj', number, toDate('2024-01-15'), CAST(concat('{"k":', toString(number), '}'), 'Nullable(JSON)')))
FROM numbers(4) ORDER BY number;

DROP DICTIONARY range_dict_sc;
DROP TABLE dict_src;
