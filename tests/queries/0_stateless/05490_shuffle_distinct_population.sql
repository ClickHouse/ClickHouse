SET allow_experimental_shuffle_query = 1;
SET max_threads = 4, max_block_size = 1000;

-- Even values occur in two streams. `DISTINCT` must remove the extra sampling chance.
SELECT count() = 10000, uniqExact(x) = 10000, avg(x % 2) BETWEEN 0.45 AND 0.55
FROM
(
    SELECT DISTINCT x FROM
    (
        SELECT number AS x FROM numbers(100000)
        UNION ALL
        SELECT number * 2 AS x FROM numbers(50000)
    )
    SHUFFLE LIMIT 10000
);

-- Cross-shard duplicates must also be removed before the final randomization.
SELECT count() = 10000, uniqExact(number) = 10000, avg(number % 2) BETWEEN 0.45 AND 0.55
FROM
(
    SELECT DISTINCT number FROM remote('127.0.0.{1,2}', numbers(100000))
    WHERE shardNum() = 1 OR number % 2 = 0
    SHUFFLE LIMIT 10000
);

-- A full shuffle with an outer limit has the same distinct population.
SELECT count() = 10000, uniqExact(number) = 10000, avg(number % 2) BETWEEN 0.45 AND 0.55
FROM
(
    SELECT * FROM
    (
        SELECT DISTINCT number FROM remote('127.0.0.{1,2}', numbers(100000))
        WHERE shardNum() = 1 OR number % 2 = 0
        SHUFFLE
    )
    LIMIT 10000
);

-- Computed `LIMIT BY` expressions must run on the finalizer, including after-aggregation stages.
SELECT count() = 1000, uniqExact(number) = 1000
FROM
(
    SELECT DISTINCT number FROM remote('127.0.0.{1,2}', numbers(10000))
    SHUFFLE LIMIT 1 BY number % 1000
)
SETTINGS distributed_group_by_no_merge = 2, distributed_push_down_limit = 0;

SELECT count() = 1000, uniqExact(number) = 1000
FROM
(
    SELECT DISTINCT number FROM remote('127.0.0.{1,2}', numbers(10000))
    SHUFFLE LIMIT 1 BY number % 1000
)
SETTINGS distributed_group_by_no_merge = 2, distributed_push_down_limit = 1;
