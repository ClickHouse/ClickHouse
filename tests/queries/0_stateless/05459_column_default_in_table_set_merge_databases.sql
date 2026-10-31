-- The unqualified table in `ALIAS p IN keys` refers to the database of the table that declares the
-- column, also when the column is computed by a `Merge` table spanning several databases: the
-- children must not share the set of one of them, nor resolve `keys` in the current database.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE keys (p UInt8) ENGINE = MergeTree ORDER BY p;
INSERT INTO keys VALUES (1);
CREATE TABLE t (p UInt8, flag UInt8 ALIAS p IN keys) ENGINE = MergeTree ORDER BY p;
INSERT INTO t VALUES (1), (2), (3);

USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE keys (p UInt8) ENGINE = MergeTree ORDER BY p;
INSERT INTO keys VALUES (2);
CREATE TABLE t (p UInt8, flag UInt8 ALIAS p IN keys) ENGINE = MergeTree ORDER BY p;
INSERT INTO t VALUES (1), (2), (3);

USE {CLICKHOUSE_DATABASE:Identifier};

SELECT 'direct';
SELECT p, flag FROM t ORDER BY p;
SELECT p, flag FROM {CLICKHOUSE_DATABASE_1:Identifier}.t ORDER BY p;

-- `REGEXP` matches both databases, `test_xxx` and `test_xxx_1`.
SELECT 'merge';
SELECT _database = {CLICKHOUSE_DATABASE:String} AS first, p, flag
FROM merge(REGEXP({CLICKHOUSE_DATABASE:String}), '^t$')
ORDER BY first DESC, p;

SELECT 'merge from another current database';
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT _database = {CLICKHOUSE_DATABASE:String} AS first, p, flag
FROM merge(REGEXP({CLICKHOUSE_DATABASE:String}), '^t$')
ORDER BY first DESC, p;
USE {CLICKHOUSE_DATABASE:Identifier};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE t;
DROP TABLE keys;
