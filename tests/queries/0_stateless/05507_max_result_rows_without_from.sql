-- Queries without FROM read from `system.one`, the result limits must still apply to them

SET max_result_rows = 10;

SELECT arrayJoin(range(1000)) FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) FROM system.one FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT number FROM numbers(1000) UNION ALL SELECT 1 FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) SETTINGS max_result_rows = 0, max_result_bytes = 1000 FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT arrayJoin(range(10));
