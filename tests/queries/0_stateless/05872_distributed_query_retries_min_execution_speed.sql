-- Tags: distributed

-- While a retry of a distributed query is possible, the progress of the remote server is deferred on the initiator.
-- The minimum execution speed is checked on the initiator against that progress, so a query that sets it must not
-- defer the progress (and is not retried): otherwise the initiator would see no rows read until the first block
-- of the result and fail with `TOO_SLOW`, although the remote server reads several rows per second.

SELECT sum(sleepEachRow(0.2)) FROM remote('127.0.0.2', numbers(15))
SETTINGS max_block_size = 1, max_threads = 1, min_execution_speed = 1, timeout_before_checking_execution_speed = 1, distributed_query_retries = 0;

SELECT sum(sleepEachRow(0.2)) FROM remote('127.0.0.2', numbers(15))
SETTINGS max_block_size = 1, max_threads = 1, min_execution_speed = 1, timeout_before_checking_execution_speed = 1, distributed_query_retries = 1;
