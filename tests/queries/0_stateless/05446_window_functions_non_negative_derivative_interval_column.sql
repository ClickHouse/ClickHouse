-- The interval argument is read per row, so the result does not depend on how the input is split into blocks.

SELECT number, nonNegativeDerivative(number, toDateTime(number), toIntervalSecond(number + 1)) OVER (ORDER BY number) AS d
FROM numbers(4)
SETTINGS max_block_size = 64, max_threads = 1;

SELECT number, nonNegativeDerivative(number, toDateTime(number), toIntervalSecond(number + 1)) OVER (ORDER BY number) AS d
FROM numbers(4)
SETTINGS max_block_size = 1, max_threads = 1;
