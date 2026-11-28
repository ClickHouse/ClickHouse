WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(if(number % 32 = 0, toUInt64(512), toUInt64(0))))) AS value
SELECT countIf(repeat(value, 70) != arrayStringConcat(arrayWithConstant(70, value)))
FROM numbers(1024)
SETTINGS max_block_size = 1024, max_threads = 1;

WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(if(number % 32 = 0, toUInt64(544), toUInt64(0))))) AS value
SELECT countIf(repeat(value, 70) != arrayStringConcat(arrayWithConstant(70, value)))
FROM numbers(1024)
SETTINGS max_block_size = 1024, max_threads = 1;

WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(if(number % 32 = 0, toUInt64(1024), toUInt64(0))))) AS value
SELECT countIf(repeat(value, 70) != arrayStringConcat(arrayWithConstant(70, value)))
FROM numbers(1024)
SETTINGS max_block_size = 1024, max_threads = 1;

WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(if(number % 32 = 0, toUInt64(1024), toUInt64(14))))) AS value
SELECT countIf(repeat(value, 70) != arrayStringConcat(arrayWithConstant(70, value)))
FROM numbers(1024)
SETTINGS max_block_size = 1024, max_threads = 1;
