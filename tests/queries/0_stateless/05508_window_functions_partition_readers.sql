-- ntile, percent_rank, cume_dist, lag and lead read the whole partition: they share a window with aggregates,
-- and an explicit frame is accepted and ignored. lagInFrame and leadInFrame read the frame.

SELECT 'one window for partition readers and an aggregate';
SELECT number, ntile(2) OVER w AS bucket, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd, lag(number, 2, 100) OVER w AS behind, lead(number, 2, 100) OVER w AS ahead, sum(number) OVER w AS running
FROM numbers(5) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT 'the same under the old analyzer';
SELECT number, ntile(2) OVER w AS bucket, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd, lag(number, 2, 100) OVER w AS behind, lead(number, 2, 100) OVER w AS ahead, sum(number) OVER w AS running
FROM numbers(5) WINDOW w AS (ORDER BY number) ORDER BY number
SETTINGS enable_analyzer = 0;

SELECT 'an explicit frame changes only the aggregate';
SELECT number, ntile(2) OVER w AS bucket, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd, lag(number, 2, 100) OVER w AS behind, lead(number, 2, 100) OVER w AS ahead, sum(number) OVER w AS window_sum
FROM numbers(5) WINDOW w AS (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY number;

SELECT 'lagInFrame and leadInFrame read the frame';
SELECT number, lag(number, 2, 100) OVER w AS behind, lagInFrame(number, 2, 100) OVER w AS behind_in_frame, lead(number, 2, 100) OVER w AS ahead, leadInFrame(number, 2, 100) OVER w AS ahead_in_frame
FROM numbers(5) WINDOW w AS (ORDER BY number ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY number;

SELECT 'lag reads far behind the frame and across blocks';
SELECT countIf(l != if(number >= 1500, number - 1500, 0)), count()
FROM (SELECT number, lag(number, 1500) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS l FROM numbers(5000) SETTINGS max_block_size = 128);

SELECT 'ntile requires ORDER BY';
SELECT ntile(2) OVER () FROM numbers(4); -- { serverError BAD_ARGUMENTS }
SELECT ntile(2) OVER () FROM numbers(4) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }

SELECT 'one window step';
EXPLAIN SELECT ntile(2) OVER w, sum(number) OVER w FROM numbers(5) WINDOW w AS (ORDER BY number);
