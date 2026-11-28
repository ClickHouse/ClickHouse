-- Materializing `uuid_type_version` into constant type-name expressions of a persisted definition keeps the aliases
-- that the rest of the query refers to, and does not touch the expressions without a bare `UUID`.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SET uuid_type_version = 2;

CREATE TABLE t1 ENGINE = Memory AS SELECT CAST(1, (concat('UInt', '8') AS typename)) AS x, typename;
SELECT x, toTypeName(x), typename FROM t1;

CREATE TABLE t2 ENGINE = Memory AS SELECT CAST(1, (replaceAll('UInt16', '16', '8') AS typename)) AS x, typename;
SELECT x, toTypeName(x), typename FROM t2;

CREATE TABLE t3 ENGINE = Memory AS SELECT CAST(1, concat(('UInt' AS prefix), '8')) AS x, prefix;
SELECT x, toTypeName(x), prefix FROM t3;

-- With a bare `UUID`, the alias of the folded expression is kept on the resulting literal.
CREATE VIEW v1 AS SELECT CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0', (concat('UU', 'ID') AS typename)) AS x, typename;
SELECT toTypeName(x), typename FROM v1;

-- An expression with an alias nested inside it is left as written, so the alias is not lost; such a type name is
-- then not materialized, like any other dynamically built type name, and resolves under the setting of the query.
CREATE VIEW v2 AS SELECT CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0', concat(('UU' AS prefix), 'ID')) AS x, prefix;
SELECT toTypeName(x), prefix FROM v2;

DROP VIEW v1;
DROP VIEW v2;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
