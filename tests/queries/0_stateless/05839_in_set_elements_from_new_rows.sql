-- The first block of each subquery adds only new keys and every later block 1% of new keys, so the set used for index
-- analysis collects most of its elements by row number.
DROP TABLE IF EXISTS t_in_set_new_rows;
CREATE TABLE t_in_set_new_rows (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_in_set_new_rows SELECT number FROM numbers(200000);
SELECT count(), sum(k) FROM t_in_set_new_rows
WHERE k IN (SELECT if(number % 100 = 0, number, number % 1000) FROM numbers(200000)) SETTINGS max_block_size = 1000;

DROP TABLE IF EXISTS t_in_set_new_rows_s;
CREATE TABLE t_in_set_new_rows_s (s String) ENGINE = MergeTree ORDER BY s;
-- Seven digits, so that the order of the strings is the order of the numbers.
INSERT INTO t_in_set_new_rows_s SELECT toString(1000000 + number) FROM numbers(200000);
SELECT count(), sum(toUInt64(s) - 1000000) FROM t_in_set_new_rows_s
WHERE s IN (SELECT toString(1000000 + if(number % 100 = 0, number, number % 1000)) FROM numbers(200000)) SETTINGS max_block_size = 1000;

DROP TABLE t_in_set_new_rows;
DROP TABLE t_in_set_new_rows_s;

-- The array elements a set collects by row number take memory proportional to the selected arrays, not to the whole block.
DROP TABLE IF EXISTS t_in_set_new_rows_a;
CREATE TABLE t_in_set_new_rows_a (k UInt64, a Array(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_in_set_new_rows_a SELECT number, arrayResize([toUInt8(number)], 4096, toUInt8(0)) FROM numbers(2);
SELECT count() FROM t_in_set_new_rows_a
WHERE (k, a) IN (SELECT intDiv(number, 1000), arrayResize([toUInt8(intDiv(number, 1000))], 4096, toUInt8(0)) FROM numbers(2000))
SETTINGS max_block_size = 1000, max_threads = 1, max_memory_usage = 20000000;
DROP TABLE t_in_set_new_rows_a;
