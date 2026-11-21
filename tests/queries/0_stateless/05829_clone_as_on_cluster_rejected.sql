-- Tags: no-replicated-database
-- Tag no-replicated-database: `ON CLUSTER is not allowed for Replicated database`

-- The initiator normalizes the query and the hosts do not copy the data, so `CLONE AS` with `ON CLUSTER` is rejected
-- instead of creating or replacing the table with an empty one.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src VALUES (1);

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.dst ON CLUSTER test_shard_localhost CLONE AS src; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dst';

CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dst VALUES (2), (3);

CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE:Identifier}.dst ON CLUSTER test_shard_localhost CLONE AS src; -- { serverError SUPPORT_IS_DISABLED }
REPLACE TABLE {CLICKHOUSE_DATABASE:Identifier}.dst ON CLUSTER test_shard_localhost CLONE AS src; -- { serverError SUPPORT_IS_DISABLED }
SELECT x FROM dst ORDER BY x;

-- Without `ON CLUSTER` the data is copied.
CREATE OR REPLACE TABLE dst CLONE AS src;
SELECT x FROM dst ORDER BY x;

DROP TABLE dst;
DROP TABLE src;
