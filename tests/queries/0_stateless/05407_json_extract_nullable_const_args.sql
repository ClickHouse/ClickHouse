-- A constant NULL of a Nullable type combined with non-constant arguments must return
-- the default value for all rows when the result type cannot be inside Nullable.
-- See https://github.com/ClickHouse/ClickHouse/pull/126570

-- A constant NULL path with a non-constant JSON
SELECT JSONExtractKeys(j, CAST(NULL AS Nullable(String))) FROM values('j String', '{"":{"x":1}}', '{"":{"y":2}}');
SELECT JSONExtractKeysAndValuesRawCaseInsensitive(j, CAST(NULL AS Nullable(String))) FROM values('j String', '{"":{"x":1}}', '{"":{"y":2}}');
SELECT JSONExtract(j, CAST(NULL AS Nullable(String)), 'Array(Int64)') FROM values('j String', '{"":[1]}', '{"":[2]}');
SELECT JSONExtract(j, CAST(NULL AS Nullable(String)), 'LowCardinality(String)') AS r, toTypeName(r) FROM values('j String', '{"":"x"}', '{"":"y"}');
SELECT JSONExtract(j, CAST(NULL AS Nullable(String)), 'LowCardinality(Nullable(String))') AS r, toTypeName(r) FROM values('j String', '{"":"x"}', '{"":"y"}');

-- A constant NULL JSON with a non-constant path
SELECT JSONExtractKeys(CAST(NULL AS Nullable(String)), k) FROM values('k String', 'a', 'b');
SELECT JSONExtract(CAST(NULL AS Nullable(String)), k, 'Array(Int64)') FROM values('k String', 'a', 'b');
SELECT JSONExtract(CAST(NULL AS Nullable(String)), k, 'LowCardinality(String)') AS r, toTypeName(r) FROM values('k String', 'a', 'b');

-- A constant non-NULL Nullable argument does not affect the result
SELECT JSONExtractKeys(j, CAST('a' AS Nullable(String))) FROM values('j String', '{"a":{"x":1}}', '{"a":{"y":2}}');
SELECT JSONExtractKeys(CAST('{"a":{"x":1},"b":{"y":2}}' AS Nullable(String)), k) FROM values('k String', 'a', 'b');
