-- A `RENAME` of a table defined by an unresolved table function must not resolve it while
-- holding the database mutex: `view` looks up its source table in the same database.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS v2;

CREATE TABLE src (x UInt64) ENGINE = Memory;
INSERT INTO src VALUES (1);
CREATE TABLE v (x UInt64) AS view(SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.src);

RENAME TABLE v TO v2;
SELECT * FROM v2;

DROP TABLE v2;
DROP TABLE src;
