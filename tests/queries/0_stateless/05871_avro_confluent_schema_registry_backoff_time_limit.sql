-- Tags: no-fasttest
-- no-fasttest: needs Avro
-- An expired `max_execution_time` stops the backoff between two schema registry requests. Nothing listens on port 1.

SELECT * FROM format(AvroConfluent, 'a Int32', unhex('0000000001'))
SETTINGS format_avro_schema_registry_url = 'http://127.0.0.1:1', format_avro_schema_registry_max_retries = 1,
    format_avro_schema_registry_retry_initial_backoff_ms = 10000, max_execution_time = 1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

INSERT INTO FUNCTION file(currentDatabase() || '_05871.avro', 'AvroConfluent', 'a Int32') SELECT 1
SETTINGS output_format_avro_confluent_subject = 's', format_avro_schema_registry_url = 'http://127.0.0.1:1',
    format_avro_schema_registry_max_retries = 1, format_avro_schema_registry_retry_initial_backoff_ms = 10000,
    max_execution_time = 1, timeout_overflow_mode = 'throw', engine_file_truncate_on_insert = 1; -- { serverError TIMEOUT_EXCEEDED }

-- Without the fix both queries wait out the 10 second backoff.
SYSTEM FLUSH LOGS query_log;
SELECT exception_code, query_duration_ms < 8000 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND exception_code != 0
ORDER BY event_time_microseconds;
