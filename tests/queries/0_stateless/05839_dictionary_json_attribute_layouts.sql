-- `JSON` and `Nullable(JSON)` dictionary attributes in every layout that supports them: a key with a NULL value,
-- a key with a value and a missing key, through `dictGet`, `dictGetOrDefault` with a constant default
-- and `dictGetOrDefault` with a non-constant default (short-circuit evaluation).

SET short_circuit_function_evaluation_for_nulls_threshold = 1;

CREATE TABLE json_source (id UInt64, start_date Date, end_date Date, j JSON, nj Nullable(JSON)) ENGINE = Memory;
-- The NULL row comes first, so it is the first value loaded.
INSERT INTO json_source VALUES (1, '2024-01-01', '2024-01-31', '{"a":1}', NULL), (2, '2024-01-01', '2024-01-31', '{"a":2}', '{"b":2}');

CREATE DICTIONARY json_flat (id UInt64, j JSON, nj Nullable(JSON))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'json_source')) LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY json_hashed (id UInt64, j JSON, nj Nullable(JSON))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'json_source')) LAYOUT(HASHED()) LIFETIME(0);

CREATE DICTIONARY json_hashed_array (id UInt64, j JSON, nj Nullable(JSON))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'json_source')) LAYOUT(HASHED_ARRAY()) LIFETIME(0);

CREATE DICTIONARY json_range_hashed (id UInt64, start_date Date, end_date Date, j JSON, nj Nullable(JSON))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'json_source')) LAYOUT(RANGE_HASHED()) RANGE(MIN start_date MAX end_date) LIFETIME(0);

SELECT 'FLAT';
SELECT id,
    toString(dictGet('json_flat', 'j', id)),
    toString(dictGet('json_flat', 'nj', id)),
    toString(dictGetOrDefault('json_flat', 'j', id, CAST('{"c":0}', 'JSON'))),
    toString(dictGetOrDefault('json_flat', 'nj', id, CAST('{"c":0}', 'Nullable(JSON)'))),
    toString(dictGetOrDefault('json_flat', 'j', id, CAST(concat('{"d":', toString(id), '}'), 'JSON'))),
    toString(dictGetOrDefault('json_flat', 'nj', id, CAST(concat('{"d":', toString(id), '}'), 'Nullable(JSON)')))
FROM (SELECT number AS id FROM numbers(1, 3));

SELECT 'HASHED';
SELECT id,
    toString(dictGet('json_hashed', 'j', id)),
    toString(dictGet('json_hashed', 'nj', id)),
    toString(dictGetOrDefault('json_hashed', 'j', id, CAST('{"c":0}', 'JSON'))),
    toString(dictGetOrDefault('json_hashed', 'nj', id, CAST('{"c":0}', 'Nullable(JSON)'))),
    toString(dictGetOrDefault('json_hashed', 'j', id, CAST(concat('{"d":', toString(id), '}'), 'JSON'))),
    toString(dictGetOrDefault('json_hashed', 'nj', id, CAST(concat('{"d":', toString(id), '}'), 'Nullable(JSON)')))
FROM (SELECT number AS id FROM numbers(1, 3));

SELECT 'HASHED_ARRAY';
SELECT id,
    toString(dictGet('json_hashed_array', 'j', id)),
    toString(dictGet('json_hashed_array', 'nj', id)),
    toString(dictGetOrDefault('json_hashed_array', 'j', id, CAST('{"c":0}', 'JSON'))),
    toString(dictGetOrDefault('json_hashed_array', 'nj', id, CAST('{"c":0}', 'Nullable(JSON)'))),
    toString(dictGetOrDefault('json_hashed_array', 'j', id, CAST(concat('{"d":', toString(id), '}'), 'JSON'))),
    toString(dictGetOrDefault('json_hashed_array', 'nj', id, CAST(concat('{"d":', toString(id), '}'), 'Nullable(JSON)')))
FROM (SELECT number AS id FROM numbers(1, 3));

SELECT 'RANGE_HASHED';
SELECT id,
    toString(dictGet('json_range_hashed', 'j', id, toDate('2024-01-15'))),
    toString(dictGet('json_range_hashed', 'nj', id, toDate('2024-01-15'))),
    toString(dictGetOrDefault('json_range_hashed', 'j', id, toDate('2024-01-15'), CAST('{"c":0}', 'JSON'))),
    toString(dictGetOrDefault('json_range_hashed', 'nj', id, toDate('2024-01-15'), CAST('{"c":0}', 'Nullable(JSON)'))),
    toString(dictGetOrDefault('json_range_hashed', 'j', id, toDate('2024-01-15'), CAST(concat('{"d":', toString(id), '}'), 'JSON'))),
    toString(dictGetOrDefault('json_range_hashed', 'nj', id, toDate('2024-01-15'), CAST(concat('{"d":', toString(id), '}'), 'Nullable(JSON)')))
FROM (SELECT number AS id FROM numbers(1, 3));

-- `IP_TRIE` does not support `Nullable` attributes.
CREATE TABLE json_ip_source (prefix String, j JSON) ENGINE = Memory;
INSERT INTO json_ip_source VALUES ('10.0.0.0/8', '{"a":1}'), ('192.168.0.0/16', '{"a":2}');

CREATE DICTIONARY json_ip_trie (prefix String, j JSON)
PRIMARY KEY prefix SOURCE(CLICKHOUSE(TABLE 'json_ip_source')) LAYOUT(IP_TRIE()) LIFETIME(0);

SELECT 'IP_TRIE';
SELECT ip,
    toString(dictGet('json_ip_trie', 'j', ip)),
    toString(dictGetOrDefault('json_ip_trie', 'j', ip, CAST('{"c":0}', 'JSON'))),
    toString(dictGetOrDefault('json_ip_trie', 'j', ip, CAST(concat('{"d":"', toString(ip), '"}'), 'JSON')))
FROM (SELECT arrayJoin([toIPv4('10.1.1.1'), toIPv4('192.168.1.1'), toIPv4('8.8.8.8')]) AS ip);

-- `JSON` nested in `Array` and `Map`, with a different path in every row.
CREATE TABLE json_nested_source (id UInt64, a Array(JSON), m Map(String, JSON)) ENGINE = Memory;
INSERT INTO json_nested_source SELECT number, [concat('{"p', toString(number), '":', toString(number), '}')::JSON], map('k', concat('{"q', toString(number), '":', toString(number), '}')::JSON) FROM numbers(3);

CREATE DICTIONARY json_nested_hashed (id UInt64, a Array(JSON), m Map(String, JSON))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'json_nested_source')) LAYOUT(HASHED()) LIFETIME(0);

SELECT 'nested';
SELECT id, toString(dictGet('json_nested_hashed', 'a', id)), toString(dictGet('json_nested_hashed', 'm', id))
FROM (SELECT number AS id FROM numbers(4));

-- `JSON` values inside `Dynamic`, with a different path in every row.
CREATE TABLE json_dynamic_source (id UInt64, a Array(Dynamic)) ENGINE = Memory;
INSERT INTO json_dynamic_source SELECT number, [concat('{"p', toString(number), '":', toString(number), '}')::JSON::Dynamic, number::Dynamic] FROM numbers(3);

CREATE DICTIONARY json_dynamic_hashed (id UInt64, a Array(Dynamic))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'json_dynamic_source')) LAYOUT(HASHED()) LIFETIME(0);

SELECT 'dynamic';
SELECT id, toString(dictGet('json_dynamic_hashed', 'a', id)), arrayMap(x -> dynamicType(x), dictGet('json_dynamic_hashed', 'a', id))
FROM (SELECT number AS id FROM numbers(4));
