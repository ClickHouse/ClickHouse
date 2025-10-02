SET max_block_size = 10;
SET max_result_rows = 20;
SET result_overflow_mode = 'throw';

SELECT DISTINCT intDiv(number, 10) FROM numbers(300); -- { serverError 396 }
SELECT DISTINCT intDiv(number, 10) FROM numbers(190);
SELECT DISTINCT intDiv(number, 10) FROM numbers(200);
SELECT DISTINCT intDiv(number, 10) FROM numbers(210); -- { serverError 396 }

SET result_overflow_mode = 'break';

SELECT DISTINCT intDiv(number, 10) FROM numbers(300);
SELECT DISTINCT intDiv(number, 10) FROM numbers(190);
SELECT DISTINCT intDiv(number, 10) FROM numbers(200);
SELECT DISTINCT intDiv(number, 10) FROM numbers(210);

SET max_block_size = 10;
SET max_result_rows = 1;
SELECT number FROM system.numbers;
SELECT count() FROM numbers(100);
-- subquery result is not the total result
SELECT count() FROM (SELECT * FROM numbers(100));
-- remote query result is not the total result
SELECT count() FROM remote('127.0.0.{1,2}', numbers(100));
