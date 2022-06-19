Trace visualizer is a tool for representation of a tracing data as a Gantt diagram.

# Quick start
For now this tool is not integrated into ClickHouse and requires a lot of manual adjustments.
```bash
cd utils/trace-visualizer
python3 -m http.server
```
Open [localhost](http://localhost:8000). It will show an example of data. To show your tracing data you have to put it in JSON format near `index.html` and change call to `fetchData()` function at the bottom of `index.html`. (Do not forget to disable browser caching while changing it).

# Visualizing query trace
First of all [opentelemetry_span_log](https://clickhouse.com/docs/en/operations/opentelemetry/) system table must be enabled to save query traces. Then run a query you want to trace with a setting:
```sql
set opentelemetry_start_trace_probability=1;
SELECT 1;
```

To find out `trace_id` of a query run the following command:
```sql
SELECT DISTINCT trace_id FROM system.opentelemetry_span_log ORDER BY query_start_time DESC;
```

To obtain JSON data suitable for visualizing run:
```sql
SELECT tuple (parent_span_id, attribute['clickhouse.thread_id'] || attribute['thread_number'] as thread_id)::Tuple(parent_span_id UInt64, thread_id String) as group, operation_name, start_time_us, finish_time_us, sipHash64(operation_name) as color, attribute
from system.opentelemetry_span_log
WHERE trace_id = 'your-trace-id'
ORDER BY group ASC
FORMAT JSON SETTINGS output_format_json_named_tuples_as_objects = 1;
```

# Dependencies
  1. [D3js](https://github.com/d3/d3) (v4).
  2. [Tooltips for D3](https://github.com/caged/d3-tip).
  3. [jquery](https://github.com/jquery/jquery).
  4. [Bootstrap](https://github.com/twbs/bootstrap).
