-- Tags: shard

-- `skip_unavailable_shards` recognizes which table is missing on the shard by the exception.
-- `INSERT` does not check the table before sending the query, so the shard has to throw it.

SET skip_unavailable_shards = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0;
SET parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t (x UInt8) ENGINE = Memory AS SELECT 1;
CREATE TABLE dist AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE no_table AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), `ab-se'nt`);
CREATE TABLE no_table_in_default_database AS t ENGINE = Distributed(test_shard_localhost, '', absent_05642);
CREATE TABLE no_database AS t ENGINE = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE_1:Identifier}, t);

INSERT INTO no_table VALUES (1);
INSERT INTO no_table SELECT x FROM dist;
INSERT INTO dist SELECT x FROM no_table;
INSERT INTO no_table_in_default_database VALUES (1);
INSERT INTO dist SELECT x FROM no_table_in_default_database;
INSERT INTO no_database VALUES (1);
INSERT INTO dist SELECT x FROM no_database;

-- Other tables with similar names.
INSERT INTO dist SELECT x FROM dist WHERE x IN (SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.t2); -- { serverError UNKNOWN_TABLE }
INSERT INTO dist SELECT x FROM dist WHERE x IN (SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.`t'2`); -- { serverError UNKNOWN_TABLE }
INSERT INTO dist SELECT x FROM dist WHERE x IN (SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.t); -- { serverError UNKNOWN_DATABASE }

SELECT count() FROM t;
