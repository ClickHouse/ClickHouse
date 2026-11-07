-- Under a frame that starts at the partition start, the rows the frame has passed are no longer kept,
-- so one long partition does not have to fit in memory.
SET max_memory_usage = 50000000;
SELECT max(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(10000000) FORMAT Null;

-- A sliding frame over strings rebuilds its state per row; the memory of the old states is given back
-- long before the partition ends.
SET max_memory_usage = 200000000;
SELECT any(repeat('x', 100)) OVER (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) FROM numbers(5000000) FORMAT Null;
