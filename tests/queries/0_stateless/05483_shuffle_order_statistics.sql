SET allow_experimental_shuffle_query = 1;
SET query_plan_use_shuffle_for_order_by_rand = 1;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;
SET max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0;
SET max_threads = 1, max_block_size = 1000;

-- The first 10000 of 100000 uniform keys have mean about 0.05 and maximum about 0.1.
-- Loose bounds check the distribution, not just ordering, across multiple output chunks.
-- Cast before averaging: hoisting division out of `avg` must not leave an overflowing `UInt64` sum.
SELECT count() = 10000, avg(toFloat64(r) / 4294967296.) BETWEEN 0.04 AND 0.06,
    max(r / 4294967296.) BETWEEN 0.08 AND 0.12
FROM (SELECT rand() AS r FROM numbers(100000) ORDER BY r LIMIT 10000);

SELECT count() = 10000, avg(toFloat64(r) / 18446744073709551616.) BETWEEN 0.04 AND 0.06,
    max(r / 18446744073709551616.) BETWEEN 0.08 AND 0.12
FROM (SELECT rand64() AS r FROM numbers(100000) ORDER BY r LIMIT 10000);

SELECT count() = 10000, avg(toFloat64(r) / 18446744073709551616.) BETWEEN 0.94 AND 0.96,
    min(r / 18446744073709551616.) BETWEEN 0.88 AND 0.92
FROM (SELECT rand64() AS r FROM numbers(100000) ORDER BY r DESC LIMIT 10000);

-- Unequal shard populations: 10000 and 90000. The smaller shard must supply about 10%, not 50%, of the sample.
SELECT count() = 10000, abs(avg(s = 1) - 0.1) < 0.025
FROM
(
    SELECT shardNum() AS s, number
    FROM remote('127.0.0.{1,2}', numbers(90000))
    WHERE number < if(shardNum() = 1, 10000, 90000)
    SHUFFLE LIMIT 10000
);

SELECT count() = 10000, abs(avg(s = 1) - 0.1) < 0.025
FROM
(
    SELECT shardNum() AS s, number
    FROM remote('127.0.0.{1,2}', numbers(90000))
    WHERE number < if(shardNum() = 1, 10000, 90000)
    ORDER BY rand64() LIMIT 10000
);
