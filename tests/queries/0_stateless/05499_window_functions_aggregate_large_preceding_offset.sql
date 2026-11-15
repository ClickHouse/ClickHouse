-- A frame start far before the partition stays at the first row, so every row adds only the rows that
-- entered the frame instead of aggregating the whole frame again.
SET max_execution_time = 20;
SELECT sum(number) OVER (ROWS BETWEEN 1000000000 PRECEDING AND CURRENT ROW) FROM numbers(2000000) FORMAT Null;
