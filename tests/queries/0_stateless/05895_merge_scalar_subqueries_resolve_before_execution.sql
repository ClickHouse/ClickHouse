SET session_timezone = 'UTC';

CREATE TABLE ms_deferred (k UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ms_deferred VALUES (0);

-- The whole SELECT list is resolved before a mergeable scalar subquery is executed.
SELECT
    (SELECT sum(k) FROM ms_deferred WHERE k >= 0),
    (SELECT count() FROM ms_deferred WHERE missing_column = 0)
SETTINGS max_rows_to_read = 0; -- { serverError UNKNOWN_IDENTIFIER }

-- Mergeable subqueries inside another subquery, behind an alias, as a function parameter and as a constant argument.
SELECT (SELECT count() FROM ms_deferred WHERE k <= (SELECT max(k) FROM ms_deferred WHERE k >= 0)), (SELECT sum(k) FROM ms_deferred WHERE k >= 0);
SELECT (SELECT count() FROM ms_deferred WHERE k >= 0) AS c, c + 1, (SELECT sum(k) FROM ms_deferred WHERE k >= 0);
SELECT quantile((SELECT avg(k) FROM ms_deferred WHERE k >= 0))(number), (SELECT count() FROM ms_deferred WHERE k >= 0) FROM numbers(1);
SELECT toDateTime64((SELECT count() FROM ms_deferred WHERE k >= 0), (SELECT max(k) FROM ms_deferred WHERE k >= 0));

DROP TABLE ms_deferred;
