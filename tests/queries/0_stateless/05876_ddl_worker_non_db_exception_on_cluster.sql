-- Tags: no-replicated-database
-- An ON CLUSTER entry whose execution throws an exception that is not a DB::Exception is finished with that error,
-- and the next ON CLUSTER entry is executed.

SET distributed_ddl_output_mode = 'throw';

CREATE TABLE t_bad_uri ON CLUSTER test_shard_localhost (a String) ENGINE = URL('http://[::1/x', 'CSV') FORMAT Null; -- { serverError POCO_EXCEPTION }
CREATE TABLE t_after ON CLUSTER test_shard_localhost (x Int8) ENGINE = MergeTree ORDER BY x FORMAT Null;

SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t_bad_uri', 't_after');

DROP TABLE t_after ON CLUSTER test_shard_localhost SYNC FORMAT Null;
