-- Statistical checks that `SHUFFLE` returns a uniformly random order and a uniformly random sample,
-- including when several streams are merged. The bounds are very loose (more than 10 standard deviations).

SET allow_experimental_shuffle_query = 1;
SET max_threads = 8, max_block_size = 1000;

-- The order is not the original one.
SELECT groupArray(number) != range(1000) FROM (SELECT number FROM numbers_mt(1000) SHUFFLE);
SELECT groupArray(number) != range(1000) FROM (SELECT number FROM numbers(1000) SHUFFLE SETTINGS max_threads = 1);

-- The position of a row is not correlated with its value (the standard deviation of the correlation is 0.001).
SELECT abs(corr(toFloat64(number), toFloat64(pos))) < 0.02
FROM (SELECT number, rowNumberInAllBlocks() AS pos FROM (SELECT number FROM numbers_mt(1000000) SHUFFLE));

-- Every value is equally likely to be in any part of the result (the standard deviation of every count is about 95).
SELECT count() = 100, min(c) > 9000, max(c) < 11000
FROM
(
    SELECT intDiv(pos, 100000) * 10 + intDiv(number, 100000) AS cell, count() AS c
    FROM (SELECT number, rowNumberInAllBlocks() AS pos FROM (SELECT number FROM numbers_mt(1000000) SHUFFLE))
    GROUP BY cell
);

-- A sample with LIMIT is uniform: every value has the same probability to be selected (the standard deviation of every count is about 95).
SELECT count() = 10, min(c) > 9000, max(c) < 11000
FROM
(
    SELECT intDiv(number, 100000) AS bucket, count() AS c
    FROM (SELECT number FROM numbers_mt(1000000) SHUFFLE LIMIT 100000)
    GROUP BY bucket
);

-- Distributed: the rows from both servers are interleaved uniformly (the standard deviation is 0.016).
SELECT abs(avg(s = 1) - 0.5) < 0.1
FROM
(
    SELECT s, rowNumberInAllBlocks() AS pos
    FROM (SELECT shardNum() AS s, number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE)
)
WHERE pos < 1000;

-- A small sample of a large stream: every stream skips most rows (sampling the number of rows to skip).
-- Every value has the same probability to be selected: the expected count in every bucket is 100 (the standard deviation is about 9.5),
-- and the expected average is 5000000 (the standard deviation is about 91000).
SELECT count() = 10, min(c) > 50, max(c) < 150, abs(sum(s) / 1000 - 5000000) < 500000
FROM
(
    SELECT intDiv(number, 1000000) AS bucket, count() AS c, sum(number) AS s
    FROM (SELECT number FROM numbers_mt(10000000) SHUFFLE LIMIT 1000)
    GROUP BY bucket
);
