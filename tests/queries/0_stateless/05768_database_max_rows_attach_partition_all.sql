-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the table engine is replaced.

-- `ATTACH PARTITION ALL FROM` (also used by `CREATE TABLE ... CLONE AS`) is checked against the database
-- `max_rows` limit as a whole: it is rejected without attaching anything even if the first partition alone
-- fits, and the partitions that are already attached are not charged again.
-- The source table lives in the current database, which has no limit.

CREATE TABLE src (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
SYSTEM STOP MERGES src;
INSERT INTO src SELECT intDiv(number, 20), number FROM numbers(40);

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_rows = 100;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.filler SELECT number FROM numbers(70);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst (p UInt8, x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst', 'r1') PARTITION BY p ORDER BY x;

SELECT '-- 30 rows of headroom: each partition of 20 rows fits alone, both together do not';
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst ATTACH PARTITION ALL FROM src; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.dst;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.clone CLONE AS src
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/clone', 'r1') PARTITION BY p ORDER BY x; -- { serverError TOO_MANY_ROWS }
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

SELECT '-- an already attached partition is not charged again';
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst ATTACH PARTITION 0 FROM src;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst ATTACH PARTITION ALL FROM src; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.dst;
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_rows = 110;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst ATTACH PARTITION ALL FROM src;
SELECT p, count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.dst GROUP BY p ORDER BY p;
SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

SELECT '-- repeating it at the limit is a no-op';
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst ATTACH PARTITION ALL FROM src;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.dst;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
