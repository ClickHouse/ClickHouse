#!/usr/bin/env python3
"""DataFusion worker that executes Substrait plans and returns results via Arrow IPC."""

import sys
import json
import pyarrow as pa
import pyarrow.ipc as ipc
from datafusion import SessionContext
from datafusion.substrait import Serde, Consumer


def main():
    if len(sys.argv) != 4:
        print("Usage: datafusion_worker.py <substrait_plan> <tables_json> <output_arrow>", file=sys.stderr)
        sys.exit(1)

    substrait_path = sys.argv[1]
    tables_json = sys.argv[2]
    output_path = sys.argv[3]

    # Load table mappings: {"table_name": "csv_path", ...}
    tables = json.loads(tables_json)

    ctx = SessionContext()
    
    # Register all tables
    for table_name, csv_path in tables.items():
        ctx.register_csv(table_name, csv_path, has_header=True)

    with open(substrait_path, "rb") as f:
        substrait_bytes = f.read()

    plan = Serde.deserialize_bytes(substrait_bytes)
    logical_plan = Consumer.from_substrait_plan(ctx, plan)
    df = ctx.create_dataframe_from_logical_plan(logical_plan)

    batches = df.collect()
    table = pa.Table.from_batches(batches)

    with pa.OSFile(output_path, "wb") as sink:
        writer = ipc.new_file(sink, table.schema)
        writer.write_table(table)
        writer.close()


if __name__ == "__main__":
    main()
