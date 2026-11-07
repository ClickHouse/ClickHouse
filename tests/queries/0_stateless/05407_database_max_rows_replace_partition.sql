-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the table engine is replaced.

-- `REPLACE PARTITION FROM` must be charged against the database `max_rows` limit for the net change:
-- the incoming rows minus the rows of the replaced destination partition. A replacement that does
-- not fit must be rejected before anything is published. The source tables live in the current
-- database, which has no limit.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_rows = 100;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.filler SELECT number FROM numbers(60);

-- `MergeTree`
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT 1, number FROM numbers(30);
CREATE TABLE src50 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO src50 SELECT 1, number FROM numbers(50);
CREATE TABLE src40 (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO src40 SELECT 1, number FROM numbers(40);

-- 60 + 30 - 30 + 50 = 110 rows exceed the limit.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t REPLACE PARTITION 1 FROM src50; -- { serverError TOO_MANY_ROWS }
SELECT 'MergeTree rejected', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- 60 + 30 - 30 + 40 = 100 rows fit.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t REPLACE PARTITION 1 FROM src40;
SELECT 'MergeTree replaced', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t SYNC;

-- `ReplicatedMergeTree`
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.r (p UInt8, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r1') PARTITION BY p ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.r SELECT 1, number FROM numbers(30);
CREATE TABLE rsrc50 (p UInt8, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rsrc50', 'r1') PARTITION BY p ORDER BY x;
INSERT INTO rsrc50 SELECT 1, number FROM numbers(50);
CREATE TABLE rsrc40 (p UInt8, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rsrc40', 'r1') PARTITION BY p ORDER BY x;
INSERT INTO rsrc40 SELECT 1, number FROM numbers(40);

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.r REPLACE PARTITION 1 FROM rsrc50; -- { serverError TOO_MANY_ROWS }
SELECT 'ReplicatedMergeTree rejected', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.r;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.r REPLACE PARTITION 1 FROM rsrc40;
SELECT 'ReplicatedMergeTree replaced', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.r;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
