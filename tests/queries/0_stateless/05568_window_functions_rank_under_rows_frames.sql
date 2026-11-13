-- Peer groups are the rows tied on ORDER BY whatever the frame type: rank and dense_rank see the same peers
-- under a ROWS frame as under the default frame, while an aggregate in the same window follows the ROWS frame.

SELECT x, rank() OVER w AS r, dense_rank() OVER w AS dr, sum(x) OVER w AS s
FROM (SELECT arrayJoin([1, 1, 2, 3, 3, 3, 4]) AS x)
WINDOW w AS (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY x, s;

SELECT '-- The queries of issue 112665';
SELECT groupArray(r) FROM (SELECT rank() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));
SELECT groupArray(r) FROM (SELECT dense_rank() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));

SELECT '-- Peer groups spanning blocks';
SELECT k, rank() OVER w AS r, dense_rank() OVER w AS dr, sum(k) OVER w AS s
FROM (SELECT intDiv(number, 3) AS k FROM numbers(8))
WINDOW w AS (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY k, s
SETTINGS max_block_size = 1;

SELECT '-- Without ORDER BY the partition is one peer group';
SELECT number, rank() OVER w AS r, dense_rank() OVER w AS dr, sum(number) OVER w AS s
FROM numbers(4)
WINDOW w AS (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY number;
