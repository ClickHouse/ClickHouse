-- A dictionary host starting with `[` without the closing `]` reached a `chassert` in `DNSResolver`.
-- https://github.com/ClickHouse/ClickHouse/issues/123776

DROP DICTIONARY IF EXISTS d_05493;
DROP TABLE IF EXISTS src_05493;

CREATE TABLE src_05493 (id UInt64, v String) ENGINE = Memory;

CREATE DICTIONARY d_05493 (id UInt64, v String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST '[x' PORT 9000 TABLE 'src_05493'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT dictGet('d_05493', 'v', 1::UInt64); -- { serverError DNS_ERROR }

DROP DICTIONARY d_05493;
DROP TABLE src_05493;
