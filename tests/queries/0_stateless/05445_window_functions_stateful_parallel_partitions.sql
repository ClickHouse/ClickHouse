-- Functions that carry state from row to row give the same result when the partitions are spread over several threads.

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt32, p UInt8, v Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t SELECT number, number % 8, (number * 13) % 29 FROM numbers(400);

SELECT 'threads 1: ' || toString(cityHash64(groupArray((p, id, r))))
FROM
(
    SELECT p, id, (ntile(5) OVER n, round(exponentialTimeDecayedSum(7)(v, id) OVER d, 6), round(exponentialTimeDecayedAvg(7)(v, id) OVER d, 6), lag(v, 3) OVER w) AS r
    FROM t
    WINDOW
        w AS (PARTITION BY p ORDER BY id),
        n AS (PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
        d AS (PARTITION BY p ORDER BY id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
    ORDER BY p, id
)
SETTINGS max_threads = 1;

SELECT 'threads 4: ' || toString(cityHash64(groupArray((p, id, r))))
FROM
(
    SELECT p, id, (ntile(5) OVER n, round(exponentialTimeDecayedSum(7)(v, id) OVER d, 6), round(exponentialTimeDecayedAvg(7)(v, id) OVER d, 6), lag(v, 3) OVER w) AS r
    FROM t
    WINDOW
        w AS (PARTITION BY p ORDER BY id),
        n AS (PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
        d AS (PARTITION BY p ORDER BY id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
    ORDER BY p, id
)
SETTINGS max_threads = 4;
