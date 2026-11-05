-- Queries without FROM read from `system.one`, the result limits must still apply to them

SET max_result_rows = 100;

SELECT arrayJoin(range(1000)) FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) FROM system.one FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) SETTINGS enable_analyzer = 0 FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) FROM system.one SETTINGS enable_analyzer = 0 FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) SETTINGS max_result_rows = 0, max_result_bytes = 1000 FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT arrayJoin(range(100));
