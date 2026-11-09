-- A frame that ends before the current peer group is ready as soon as its own rows are visible: the first row
-- comes out before the whole peer group has arrived, so LIMIT stops the query within a small memory limit.

SELECT count() OVER (ORDER BY toUInt64(0) RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
FROM numbers(10000000)
LIMIT 1
SETTINGS max_memory_usage = 10000000, max_threads = 1, max_block_size = 1000;

SELECT sum(number) OVER (ORDER BY toUInt64(0) GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
FROM numbers(10000000)
LIMIT 1
SETTINGS max_memory_usage = 10000000, max_threads = 1, max_block_size = 1000;

-- rank and dense_rank read only where the peer group starts, so they do not wait for the group to end either.

SELECT rank() OVER (ORDER BY toUInt64(0) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM numbers(10000000)
LIMIT 1
SETTINGS max_memory_usage = 10000000, max_threads = 1, max_block_size = 1000;

SELECT dense_rank() OVER (ORDER BY toUInt64(0) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM numbers(10000000)
LIMIT 1
SETTINGS max_memory_usage = 10000000, max_threads = 1, max_block_size = 1000;
