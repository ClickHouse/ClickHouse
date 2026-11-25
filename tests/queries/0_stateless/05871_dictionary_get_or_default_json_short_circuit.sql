-- `dictGetOrDefault` with a non-constant default keeps `JSON` paths of the attribute in the shared data
-- when the default is evaluated with short-circuit evaluation, as without it.

CREATE TABLE src (id UInt64, j JSON, nj Nullable(JSON), a Array(JSON), m Map(String, JSON)) ENGINE = Memory;
INSERT INTO src SELECT number,
    concat('{"p', toString(number), '":', toString(number), '}'),
    if(number % 2 = 0, NULL, concat('{"p', toString(number), '":1}')),
    [concat('{"p', toString(number), '":1}')::JSON],
    map('k', concat('{"p', toString(number), '":1}')::JSON)
FROM numbers(64);

CREATE DICTIONARY d (id UInt64, j JSON, nj Nullable(JSON), a Array(JSON), m Map(String, JSON))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(HASHED()) LIFETIME(0);

-- Keys 64..71 are missing and take the default.
SELECT 'j', max(length(JSONDynamicPaths(dictGetOrDefault('d', 'j', number % 72, concat('{"d":', toString(number), '}')::JSON))))
FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'nj', max(length(JSONDynamicPaths(assumeNotNull(dictGetOrDefault('d', 'nj', number % 72, concat('{"d":', toString(number), '}')::Nullable(JSON))))))
FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'enable';

-- Without dynamic paths the result has the same size as without short-circuit evaluation. Only existing keys:
-- a default takes the type of the default expression with short-circuit evaluation and its own type without it.
SELECT 'a', (SELECT sum(byteSize(dictGetOrDefault('d', 'a', number % 64, [concat('{"d":', toString(number), '}')::JSON]))) FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'enable')
    = (SELECT sum(byteSize(dictGetOrDefault('d', 'a', number % 64, [concat('{"d":', toString(number), '}')::JSON]))) FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'disable');

SELECT 'm', (SELECT sum(byteSize(dictGetOrDefault('d', 'm', number % 64, map('k', concat('{"d":', toString(number), '}')::JSON)))) FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'enable')
    = (SELECT sum(byteSize(dictGetOrDefault('d', 'm', number % 64, map('k', concat('{"d":', toString(number), '}')::JSON)))) FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'disable');

SELECT 't', (SELECT sum(byteSize(dictGetOrDefault('d', ('j', 'a'), number % 64, (concat('{"d":', toString(number), '}')::JSON, [concat('{"d":', toString(number), '}')::JSON])))) FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'enable')
    = (SELECT sum(byteSize(dictGetOrDefault('d', ('j', 'a'), number % 64, (concat('{"d":', toString(number), '}')::JSON, [concat('{"d":', toString(number), '}')::JSON])))) FROM numbers(256) SETTINGS short_circuit_function_evaluation = 'disable');

SELECT * FROM (SELECT number % 72 AS k,
    toString(dictGetOrDefault('d', 'j', k, concat('{"d":', toString(number), '}')::JSON)) AS j,
    toString(dictGetOrDefault('d', 'nj', k, concat('{"d":', toString(number), '}')::Nullable(JSON))) AS nj,
    toString(dictGetOrDefault('d', 'a', k, [concat('{"d":', toString(number), '}')::JSON])) AS a,
    toString(dictGetOrDefault('d', 'm', k, map('k', concat('{"d":', toString(number), '}')::JSON))) AS m,
    toString(dictGetOrDefault('d', ('j', 'a'), k, (concat('{"d":', toString(number), '}')::JSON, [concat('{"d":', toString(number), '}')::JSON]))) AS t
FROM numbers(256)) WHERE k IN (0, 1, 63, 64, 71) ORDER BY k, j LIMIT 1 BY k
SETTINGS short_circuit_function_evaluation = 'enable';

DROP DICTIONARY d;
DROP TABLE src;
