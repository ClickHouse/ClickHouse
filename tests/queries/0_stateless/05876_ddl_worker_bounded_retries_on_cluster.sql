-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-fasttest, no-parallel: the retries of each failing entry hold the server's distributed DDL queue for about 15 s.
-- An ON CLUSTER entry that keeps failing with a retriable error is finished with that error after a few attempts,
-- and the next ON CLUSTER entry is executed.

SET distributed_ddl_output_mode = 'throw';

CREATE TABLE t_mem ON CLUSTER test_shard_localhost ENGINE = Memory
AS SELECT groupArray(number) AS a FROM numbers(20000000) SETTINGS max_memory_usage = 20000000 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- An exception that is not a DB::Exception is retriable too.
CREATE TABLE t_bad_uri ON CLUSTER test_shard_localhost (a String) ENGINE = URL('http://[::1/x', 'CSV') FORMAT Null; -- { serverError POCO_EXCEPTION }

CREATE TABLE t_after ON CLUSTER test_shard_localhost (x Int8) ENGINE = MergeTree ORDER BY x FORMAT Null;

SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t_mem', 't_bad_uri', 't_after');

-- Each failing entry is executed 4 times: the first attempt and 3 retries.
SYSTEM FLUSH LOGS query_log;
SELECT
    countIf(position(query, concat(currentDatabase(), '.t_mem ')) > 0),
    countIf(position(query, concat(currentDatabase(), '.t_bad_uri ')) > 0)
FROM system.query_log
WHERE event_date >= yesterday() AND type != 'QueryStart' AND startsWith(query, '/* ddl_entry=');

DROP TABLE t_after ON CLUSTER test_shard_localhost SYNC FORMAT Null;
