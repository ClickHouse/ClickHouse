-- EXPLAIN ANALYZE borrows the query thread for a nested thread group, which detaches it from the
-- query and writes its query_thread_log row. That row must carry the thread's own name, not the
-- one of the borrowed scope.
SELECT count() > 0 FROM (EXPLAIN ANALYZE SELECT 1) SETTINGS log_query_threads = 1, log_queries = 1 FORMAT Null;
SYSTEM FLUSH LOGS query_thread_log;
SELECT thread_name
FROM system.query_thread_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND query LIKE '%FORMAT Null;'
    AND thread_id = master_thread_id
ORDER BY event_time_microseconds;
