Trace visualizer is a tool for representation of a tracing data as a Gantt diagram.

# Quick start
For now this tool is not integrated into ClickHouse and requires manual actions. Open `trace-visualizer/index.html` in your browser. It will show an example of data. To visualize your data click `Load` button and select your trace data JSON file.

# Visualizing query trace
First of all [opentelemetry_span_log](https://clickhouse.com/docs/en/operations/opentelemetry/) system table must be enabled to save query traces. Then run a query you want to trace with a setting:
```sql
SET opentelemetry_start_trace_probability=1;
SELECT 1;
```

To find out `trace_id` of a query run the following command:
```sql
SELECT DISTINCT trace_id FROM system.opentelemetry_span_log ORDER BY query_start_time DESC;
```

To obtain JSON data suitable for visualizing run:
```sql
SELECT tuple (leftPad(attribute['clickhouse.thread_id'] || attribute['thread_number'], 10, '0') as thread_id, parent_span_id)::Tuple(thread_id String, parent_span_id UInt64) as group, operation_name, start_time_us, finish_time_us, sipHash64(operation_name) as color, attribute
FROM system.opentelemetry_span_log
WHERE trace_id = 'your-trace-id'
FORMAT JSON SETTINGS output_format_json_named_tuples_as_objects = 1;
```

# Dependencies
  1. [D3js](https://github.com/d3/d3) (v4).
  2. [Tooltips for D3](https://github.com/caged/d3-tip).
  3. [jquery](https://github.com/jquery/jquery).
  4. [Bootstrap](https://github.com/twbs/bootstrap).
