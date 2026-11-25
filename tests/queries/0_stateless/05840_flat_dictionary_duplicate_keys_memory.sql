-- A `FLAT` dictionary keeps one value per repeated key, and its memory must not grow with the replaced values.

CREATE TABLE source (id UInt64, arr Array(UInt64), m Map(String, UInt64), j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO source SELECT number % 2, range(64), map('k', number), concat('{"n":', toString(number), '}') FROM numbers(100000);

CREATE DICTIONARY dict (id UInt64, arr Array(UInt64), m Map(String, UInt64), j JSON)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'source')) LIFETIME(0) LAYOUT(FLAT());

SELECT number AS id, length(dictGet('dict', 'arr', id)), dictGet('dict', 'm', id)['k'] % 2 = id, dictGet('dict', 'j', id).n::UInt64 = dictGet('dict', 'm', id)['k']
FROM numbers(2);

SELECT element_count, bytes_allocated < 1000000 FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';
