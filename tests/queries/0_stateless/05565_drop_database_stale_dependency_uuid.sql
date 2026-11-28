-- The referential dependency graph keeps the UUID of the view `x` after `x` is dropped and created again.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE src (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
CREATE VIEW x AS SELECT k, v FROM src;
ALTER TABLE x MODIFY COMMENT 'c';
CREATE DICTIONARY y (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'x')) LAYOUT(FLAT()) LIFETIME(0);

SET check_table_dependencies = 0; -- The dictionary `y` depends on `x`.
DROP TABLE x SYNC;
SET check_table_dependencies = 1;
CREATE TABLE x (k UInt64, v String) ENGINE = MergeTree ORDER BY k;

SET send_logs_level = 'error'; -- The dependency graphs warn about the stale UUID.
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
