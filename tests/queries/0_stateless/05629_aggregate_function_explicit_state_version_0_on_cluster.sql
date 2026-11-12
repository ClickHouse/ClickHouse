-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: this test explicitly issues plain ON CLUSTER DDL on a regular database.

-- With entry format 2 the DDL worker pins version 0 on an unversioned state type name, which must not be refused there.
SET distributed_ddl_output_mode = 'throw';
SET distributed_ddl_entry_format_version = 2;

CREATE TABLE t ON CLUSTER test_shard_localhost (s AggregateFunction(sumMap, Array(UInt8), Array(UInt8))) ENGINE = Memory FORMAT Null;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't';

DROP TABLE t;
