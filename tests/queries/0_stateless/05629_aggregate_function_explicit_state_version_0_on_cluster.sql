-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: this test explicitly issues plain ON CLUSTER DDL on a regular database.

SET distributed_ddl_output_mode = 'throw';
SET allow_suspicious_aggregate_function_version_0 = 0;

-- The initiator checks the definition before enqueueing it, because a DDL worker does not.
CREATE TABLE t_refused ON CLUSTER test_shard_localhost (s AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8))) ENGINE = Memory FORMAT Null; -- { serverError BAD_ARGUMENTS }
