SET allow_simdjson = 1;
SET max_block_size = 64;

-- Vary row sizes so the final rows have little initialized trailing storage.
SELECT 'scalar', countIf(JSON_VALUE(j, '$.n') != toString(number))
FROM
(
    SELECT number, concat('{"n":', toString(number), ',"s":"', repeat('x', number % 129), '"}') AS j
    FROM numbers(1025)
);

SELECT 'tuple', countIf(JSON_VALUE(j, tuple('$.n', '$.s')) != tuple(toString(number), repeat('x', number % 129)))
FROM
(
    SELECT number, concat('{"n":', toString(number), ',"s":"', repeat('x', number % 129), '"}') AS j
    FROM numbers(1025)
);

SELECT 'array', countIf(JSON_VALUE(j, ['$.n', '$.s']) != [toString(number), repeat('x', number % 129)])
FROM
(
    SELECT number, concat('{"n":', toString(number), ',"s":"', repeat('x', number % 129), '"}') AS j
    FROM numbers(1025)
);

SELECT 'short-last-rows', countIf(JSON_VALUE(j, '$.n') != toString(n))
FROM
(
    SELECT tupleElement(v, 1) AS j, tupleElement(v, 2) AS n
    FROM (SELECT arrayJoin([tuple('{"n":1}', 1), tuple('{"n":2}', 2)]) AS v)
);

SELECT 'constant', JSON_VALUE('{"n":7}', '$.n') != '7';
SELECT 'materialized', countIf(JSON_VALUE(materialize('{"n":7}'), '$.n') != '7') FROM numbers(257);
SELECT 'exists', countIf(JSON_EXISTS(materialize('{"n":7}'), '$.n') != 1) FROM numbers(257);
SELECT 'query', countIf(JSON_QUERY(materialize('{"a":[1,2]}'), '$.a') != '[[1,2]]') FROM numbers(257);

SELECT 'invalid-empty-missing', countIf(JSON_VALUE(j, '$.n') != '')
FROM (SELECT arrayJoin(['', '{', '[]', 'null', '{"x":1}', '{"n":']) AS j);

SELECT 'nullable-input', NOT isNull(JSON_VALUE(CAST(NULL AS Nullable(String)), '$.n'));

SET function_json_value_return_type_allow_nullable = 1;
SELECT 'nullable-result', countIf(NOT isNull(JSON_VALUE(materialize('{}'), '$.missing'))) FROM numbers(129);
SET function_json_value_return_type_allow_nullable = 0;

-- Preserve the exception from the parser-disabled `JSON_VALUE` path.
SET allow_simdjson = 0;
SELECT JSON_VALUE(materialize('{"n":7}'), '$.n'); -- { serverError NOT_IMPLEMENTED }
SET allow_simdjson = 1;
