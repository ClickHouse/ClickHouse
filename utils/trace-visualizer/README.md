Trace visualizer is a tool for representation of a tracing data as a Gantt diagram.

# Quick start
For now this tool is not integrated into ClickHouse and requires manual actions. Open `trace-visualizer/index.html` in your browser. It will show an example of data. To visualize your data click `Load` button and select your trace data JSON file.

Single page version is available at https://trace-visualizer.clickhouse.com.


# Visualizing query trace
First of all [opentelemetry_span_log](https://clickhouse.com/docs/operations/opentelemetry/) system table must be enabled to save query traces. Then run a query you want to trace with a setting:
```sql
SET opentelemetry_start_trace_probability=1, opentelemetry_trace_processors=1;
```
After this is run, the `opentelemetry_span_log` table will be created as traces will be logged there.

Run a simple query:
```sql
SELECT 1;
```

The easiest way to extract the query trace information from a single node environment is using `collect_query_traces.sh`:
```
collect_query_traces.sh your-query-id
```
The script should create a `query_trace_your-query-id.json` file that can be imported on the above-mentioned `index.html`.

To find out `trace_id` of a query, run the following command:
```sql
SELECT DISTINCT trace_id FROM system.opentelemetry_span_log WHERE attribute['clickhouse.query_id'] = 'your-query-id' ORDER BY start_time_us DESC;
```

## Collect traces in local/development environment

To obtain JSON data suitable for visualizing run:
```sql
WITH 'your-query-id' AS my_query_id
SELECT
    ('thread #' || leftPad(attribute['clickhouse.thread_id'], 7, '0')) AS group,
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
INTO OUTFILE 'query_trace.json' TRUNCATE
FORMAT JSON
SETTINGS output_format_json_named_tuples_as_objects = 1, skip_unavailable_shards = 1
```

## Collect traces in ClickHouse Cloud/distributed queries

To obtain JSON data suitable for visualizing run:
```sql
WITH 'your-query-id' AS my_query_id
SELECT
    (substring(hostName(), length(hostName()), 1) || leftPad(greatest(attribute['clickhouse.thread_id'], attribute['thread_number']), 7, '0')) AS group,
    operation_name,
    start_time_us,
    finish_time_us,
    sipHash64(operation_name) AS color,
    attribute
FROM clusterAllReplicas('default', 'system', 'opentelemetry_span_log')
WHERE 1
  AND trace_id IN (
    SELECT trace_id
    FROM clusterAllReplicas('default', 'system', 'opentelemetry_span_log')
    WHERE (attribute['clickhouse.query_id']) IN (SELECT query_id FROM system.query_log WHERE initial_query_id = my_query_id)
  )
  AND operation_name !='query'
  AND operation_name NOT LIKE '%Pipeline%'
  AND operation_name NOT LIKE 'TCPHandler%'
  AND operation_name NOT LIKE 'Query%'
ORDER BY
    hostName() ASC,
    group ASC,
    parent_span_id ASC,
    start_time_us ASC
INTO OUTFILE 'query_trace.json' TRUNCATE
FORMAT JSON
SETTINGS output_format_json_named_tuples_as_objects = 1
```

# Dependencies
  1. [D3js](https://github.com/d3/d3) (v4).
  2. [Tooltips for D3](https://github.com/caged/d3-tip).
  3. [jquery](https://github.com/jquery/jquery).
  4. [Bootstrap](https://github.com/twbs/bootstrap).
