-- Offsets that leave the frame on both sides, lags before the partition start, and offsets larger than a block.

SELECT
    p,
    n,
    lagInFrame(n, 3, -1) OVER f AS lag_in_frame,
    leadInFrame(n, 3, -1) OVER f AS lead_in_frame,
    nth_value(n, 5) OVER f AS fifth_in_frame,
    nth_value(n, 1) OVER f AS first_in_frame,
    lag(n, 7, -1) OVER w AS lag_7,
    lead(n, 7, -1) OVER w AS lead_7,
    lag(n) OVER w AS lag_1
FROM (SELECT intDiv(number, 10) AS p, number % 10 AS n FROM numbers(20))
WINDOW
    w AS (PARTITION BY p ORDER BY n),
    f AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
ORDER BY p, n
SETTINGS max_block_size = 3;

SELECT
    p,
    n,
    lagInFrame(n, 3, -1) OVER f AS lag_in_frame,
    leadInFrame(n, 3, -1) OVER f AS lead_in_frame,
    nth_value(n, 5) OVER f AS fifth_in_frame,
    nth_value(n, 1) OVER f AS first_in_frame,
    lag(n, 7, -1) OVER w AS lag_7,
    lead(n, 7, -1) OVER w AS lead_7,
    lag(n) OVER w AS lag_1
FROM (SELECT intDiv(number, 10) AS p, number % 10 AS n FROM numbers(20))
WINDOW
    w AS (PARTITION BY p ORDER BY n),
    f AS (PARTITION BY p ORDER BY n ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
ORDER BY p, n
SETTINGS max_block_size = 1;
