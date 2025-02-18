#!/usr/bin/env bash

query="
WITH '$1' AS my_query_id
SELECT
    ('thread #' || leftPad(attribute['clickhouse.thread_id'], 6, '0')) AS group,
    replaceRegexpOne(operation_name, '(.*)_.*', '\\1') AS operation_name,
    start_time_us,
    finish_time_us,
    sipHash64(operation_name) AS color,
    attribute
FROM system.opentelemetry_span_log
WHERE 1
    AND trace_id IN (
        SELECT trace_id
        FROM system.opentelemetry_span_log
        WHERE (attribute['clickhouse.query_id']) IN (SELECT query_id FROM system.query_log WHERE initial_query_id = my_query_id)
    )
    AND operation_name !='query'
    AND operation_name NOT LIKE '%Pipeline%'
    AND operation_name NOT LIKE 'TCPHandler%'
    AND operation_name NOT LIKE 'Query%'
ORDER BY
    group ASC,
    parent_span_id ASC,
    start_time_us ASC
INTO OUTFILE 'query_trace_$1.json' TRUNCATE
FORMAT JSON
SETTINGS output_format_json_named_tuples_as_objects = 1, skip_unavailable_shards = 1
"

clickhouse client -q "SYSTEM FLUSH LOGS"
clickhouse client -q "$query"
