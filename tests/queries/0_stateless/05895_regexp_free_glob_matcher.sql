-- The files a path with globs selects do not depend on `use_regexp_free_glob_matcher`,
-- except for a `{N..M}` range of more than 100000 numbers, which no regular expression is built for.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_glob_a1.csv', CSV, 'x UInt32') SELECT 1;
INSERT INTO FUNCTION file(currentDatabase() || '_glob_b2.csv', CSV, 'x UInt32') SELECT 2;
INSERT INTO FUNCTION file(currentDatabase() || '_glob_b10.csv', CSV, 'x UInt32') SELECT 10;
INSERT INTO FUNCTION file(currentDatabase() || '_glob_1234567.csv', CSV, 'x UInt32') SELECT 1234567;

SELECT 'regular expression';
SET use_regexp_free_glob_matcher = 0;

SELECT x FROM file(currentDatabase() || '_glob_*.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_?1.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_{a,b}*.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_b{1..10}.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_b{01..10}.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_{0..2000000}.csv', CSV, 'x UInt32') ORDER BY x; -- { serverError BAD_ARGUMENTS }

SELECT 'no regular expression';
SET use_regexp_free_glob_matcher = 1;

SELECT x FROM file(currentDatabase() || '_glob_*.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_?1.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_{a,b}*.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_b{1..10}.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_b{01..10}.csv', CSV, 'x UInt32') ORDER BY x;
SELECT x FROM file(currentDatabase() || '_glob_{0..2000000}.csv', CSV, 'x UInt32') ORDER BY x;
