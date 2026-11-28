-- A left block of a partial merge join whose result exceeds `max_joined_block_size_rows` is joined in several
-- calls: the first call sorts the block, and the next ones continue from a position in it. Each left row must be
-- joined exactly once, whatever the number of calls is.

SET join_algorithm = 'partial_merge', max_threads = 1, max_block_size = 65536;

-- One left block of 65536 rows in a random order, 50 rows per key; the right table has 3 rows for each of the
-- keys from 0 to 999, so 50000 left rows match 3 rows each and 15536 left rows match nothing.
DROP TABLE IF EXISTS t_pmj_continued_left;
DROP TABLE IF EXISTS t_pmj_continued_right;
CREATE TABLE t_pmj_continued_left (v UInt64, k UInt64) ENGINE = Memory;
CREATE TABLE t_pmj_continued_right (k UInt64, r UInt64) ENGINE = Memory;
INSERT INTO t_pmj_continued_left SELECT number, intDiv(number, 50) FROM numbers(65536) ORDER BY cityHash64(number);
INSERT INTO t_pmj_continued_right SELECT number % 1000, number FROM numbers(3000);

SELECT 'inner', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a INNER JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 0;
SELECT 'inner', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a INNER JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 65409;
SELECT 'inner', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a INNER JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 1000;
SELECT 'inner', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a INNER JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 100;

SELECT 'left', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a LEFT JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 0;
SELECT 'left', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a LEFT JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 65409;
SELECT 'left', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a LEFT JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 1000;
SELECT 'left', count(), uniqExact(v), sum(v), sum(r) FROM t_pmj_continued_left AS a LEFT JOIN t_pmj_continued_right AS b USING (k) SETTINGS max_joined_block_size_rows = 100;

DROP TABLE t_pmj_continued_left;
DROP TABLE t_pmj_continued_right;
