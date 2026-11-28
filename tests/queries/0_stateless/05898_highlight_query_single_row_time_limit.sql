-- `highlightQuery` does not bound its argument by `max_query_size`, so a single row can take arbitrarily
-- long to parse. A cancellation check at the entry of each row is not enough for it: the query has to
-- stop in the middle of the row.

-- 1. One row of 22 MB under a 3 s limit. Parsing the whole row takes far longer than the 15 s that the
--    check below allows. The row is materialized so that it is parsed during execution. The parser
--    limits are lifted because otherwise `max_parser_backtracks` stops this row first.
SELECT length(highlightQuery(materialize('SELECT 1 WHERE x = 0' || repeat(repeat(' OR (y = 1)', 1000), 2000)))) /* 05898_timed */
FORMAT Null SETTINGS max_execution_time = 3, max_parser_backtracks = 0, max_parser_depth = 0; -- { serverError TIMEOUT_EXCEEDED }

-- 2. The assertion is on the CPU time rather than on the error: without a check inside the row the query
--    fails with the same `TIMEOUT_EXCEEDED`, only after the whole row is parsed. `read_rows` shows that
--    the query got as far as executing the function.
SYSTEM FLUSH LOGS query_log;

SELECT 'bounded',
       count() = 1
   AND countIf(read_rows = 0) = 0
   AND countIf(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] > 15000000) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_time > now() - INTERVAL 10 MINUTE
  AND query LIKE '%05898\_timed%'
SETTINGS enable_parallel_replicas = 0;

-- 3. A long row that is not cancelled goes through the checks inside the row and gives the usual result.
SELECT length(highlightQuery(materialize('SELECT 1 WHERE x = 0' || repeat(' OR (y = 1)', 20000))));
SELECT length(tokenizeQuery(materialize('SELECT 1 WHERE x = 0' || repeat(' OR (y = 1)', 20000))));
