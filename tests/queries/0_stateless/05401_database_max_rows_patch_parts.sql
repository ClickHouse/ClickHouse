-- Patch parts of lightweight updates do not count toward the database `max_rows`:
-- they represent mutations of existing rows, not additional rows.

SET enable_lightweight_update = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_rows = 2;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic SETTINGS max_rows = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 0);
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t SET v = 1 WHERE id = 1;

-- One row and one unapplied patch part.
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active AND name LIKE 'patch-%';
USE {CLICKHOUSE_DATABASE:Identifier};
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- One more row fits into the limit of 2.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t2 VALUES (1);
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- The table with its patch part fits into a database with the limit of 1.
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t TO {CLICKHOUSE_DATABASE_2:Identifier}.t;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_2:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
