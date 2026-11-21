-- Tags: shard, no-asan, no-tsan, no-msan
-- Tag no-asan, no-tsan, no-msan: the sanitizers replace `operator new`, so the memory owned by the states is not counted against `max_memory_usage`

-- A state of `anyLast` over a large `FixedString` owns memory allocated with `operator new`, which the memory
-- tracker counts but does not refuse, so reading and copying such states must check the limit themselves.

-- Reading states of a remote query (`SerializationAggregateFunction::deserializeBinaryBulk`).
SELECT s FROM remote('127.0.0.2', view(SELECT initializeAggregation('anyLastState', toFixedString('\0', 1048577)) AS s FROM numbers(400)))
SETTINGS max_memory_usage = '100M', max_block_size = 65536, prefer_localhost_replica = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Copying states shared by a column (`ColumnAggregateFunction::ensureOwnership`).
SELECT finalizeAggregation(materialize(initializeAggregation('anyLastState', toFixedString('\0', 1048577)))) FROM numbers(4000)
SETTINGS max_memory_usage = '100M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
