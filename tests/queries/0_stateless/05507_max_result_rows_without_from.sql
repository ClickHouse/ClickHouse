-- Result limits must apply to queries without FROM (they read `system.one`) and to the quota system tables

SET max_result_rows = 10;

SELECT arrayJoin(range(1000)) FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) FROM system.one FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT number FROM numbers(1000) UNION ALL SELECT 1 FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) FROM system.quota_usage FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT arrayJoin(range(1000)) SETTINGS max_result_rows = 0, max_result_bytes = 1000 FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT number FROM numbers(1000) UNION ALL SELECT 1 SETTINGS result_overflow_mode = 'break', max_block_size = 5, max_threads = 1;

SELECT arrayJoin(range(10));
