-- Peer groups are the rows tied on ORDER BY whatever the frame type: rank, dense_rank, percent_rank and cume_dist
-- see the same peers under a ROWS frame as under the default frame, while an aggregate in the same window follows the ROWS frame.

SELECT x, rank() OVER w AS r, dense_rank() OVER w AS dr, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd, sum(x) OVER w AS s
FROM (SELECT arrayJoin([1, 1, 2, 3, 3, 3, 4]) AS x)
WINDOW w AS (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY x, s;

SELECT '-- Peer groups spanning blocks';
SELECT k, rank() OVER w AS r, dense_rank() OVER w AS dr, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd, sum(k) OVER w AS s
FROM (SELECT intDiv(number, 3) AS k FROM numbers(8))
WINDOW w AS (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY k, s
SETTINGS max_block_size = 1;

SELECT '-- Without ORDER BY the partition is one peer group';
SELECT number, rank() OVER w AS r, dense_rank() OVER w AS dr, sum(number) OVER w AS s
FROM numbers(4)
WINDOW w AS (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY number;
