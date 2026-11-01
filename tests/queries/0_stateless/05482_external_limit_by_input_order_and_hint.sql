-- At a 1-byte threshold a spilling `LIMIT BY` spills from its first chunk, so every row below passes through
-- the spill runs and the final merge.
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_bytes_before_external_limit_by = 1;
SET max_untracked_memory = 0;
SET enable_parallel_replicas = 0;

-- Each of the 1000 keys has 20 rows. After the spill, the result is sorted back into the `ORDER BY` order by
-- its columns, and keeps the rows of the in-memory execution: the two largest values of each key.
SELECT count(), sum(v), groupArray(v) = arrayReverseSort(groupArray(v))
FROM (SELECT k, v FROM (SELECT number % 1000 AS k, number AS v FROM numbers(20000)) ORDER BY v DESC LIMIT 2 BY k);

-- A sort expression that is dropped below the `LIMIT BY` leaves the order to arrival numbers.
SELECT count(), sum(v), groupArray(v) = arrayReverseSort(groupArray(v))
FROM (SELECT k, v FROM (SELECT number % 1000 AS k, number AS v FROM numbers(20000)) ORDER BY v + 1 DESC LIMIT 2 BY k);

-- An outer `LIMIT` gives the `LIMIT BY` a hint: the merged result stops at the rows that the `LIMIT` reads,
-- its length plus its offset, and only those rows are sorted back into the input order. The window drops the
-- first row of each key, so the result starts at the second row of every key.
SELECT k, v FROM (SELECT number % 1000 AS k, number AS v FROM numbers(20000)) ORDER BY v DESC LIMIT 1 OFFSET 1 BY k LIMIT 3;
SELECT k, v FROM (SELECT number % 1000 AS k, number AS v FROM numbers(20000)) ORDER BY v DESC LIMIT 1 OFFSET 1 BY k LIMIT 2 OFFSET 2;

-- Without `ORDER BY`, the `LIMIT` reads any rows of the result.
SELECT sum(c), max(c) <= 2
FROM (SELECT k, count() AS c FROM (SELECT k FROM (SELECT number % 1000 AS k FROM numbers(20000)) LIMIT 2 BY k LIMIT 10) GROUP BY k);

-- `LIMIT ... WITH TIES` also reads the rows that tie with its last row, so it gives no hint. The first ten
-- rows tie on `intDiv(v, 10)` and belong to ten keys.
SELECT count()
FROM (SELECT k, v FROM (SELECT number % 1000 AS k, number AS v FROM numbers(20000)) ORDER BY intDiv(v, 10) DESC LIMIT 1 BY k LIMIT 3 WITH TIES);

-- `exact_rows_before_limit` makes the `LIMIT BY` read its whole input, so it gives no hint either.
SELECT k, v FROM (SELECT number % 1000 AS k, number AS v FROM numbers(20000)) ORDER BY v DESC LIMIT 2 BY k LIMIT 1
FORMAT JSONCompact SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0;

-- The `LIMIT BY` below `LIMIT -n BY` passes all rows of a group past its offset to the negative step, so it
-- gets no hint. The query has 2000 result rows.
SELECT count() FROM (SELECT k, v FROM (SELECT number % 1000 AS k, number AS v FROM numbers(20000)) ORDER BY v DESC LIMIT -2 OFFSET 1 BY k LIMIT 1000);
