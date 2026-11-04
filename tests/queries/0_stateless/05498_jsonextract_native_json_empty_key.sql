CREATE TABLE t (j JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"":{"a":42}}');

-- A leading empty key must be preserved for native JSON arguments, matching the String overload.
SELECT
    JSONExtractRaw(j, '') = '{"a":42}',
    JSONExtractRaw(j, '', 'a') = '42',
    JSONHas(j, '', 'a'),
    JSONExtractInt(j, '', 'a'),
    j['']['a']
FROM t;

-- A leading empty key must not fall back to a same-named top-level key.
SELECT JSONExtractInt(CAST('{"":{"a":42},"a":99}' AS JSON), '', 'a') = 42;

-- A single empty key must be addressable without being treated as the root path.
SELECT JSONExtractInt(CAST('{"":42}' AS JSON), '') = 42;

DROP TABLE t;

