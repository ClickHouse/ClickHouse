# paimon_row_rename {#paimon-row-rename}

A Paimon table with a `ROW` column `s` that was renamed to `r` after the first commit, so the first data file is
read under its own names and mapped to the current schema by field id. Used by
`05616_data_lake_tuple_element_after_schema_change.sh`.

Written by the program below with `pypaimon==2.1.0` (`pip install pypaimon==2.1.0`, no JVM needed) and copied from
`<warehouse>/default.db/t`.

```python
# Usage: python3 gen_paimon_row_rename.py <warehouse_dir>   (pip install pypaimon==2.1.0)
import sys

import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.schema.schema_change import SchemaChange

catalog = CatalogFactory.create({"warehouse": sys.argv[1]})
catalog.create_database("default", True)
row = pa.struct([("a", pa.int64()), ("b", pa.int64())])


def write(rows, schema):
    wb = catalog.get_table("default.t").new_batch_write_builder()
    w, c = wb.new_write(), wb.new_commit()
    w.write_arrow(pa.Table.from_pylist(rows, schema=schema))
    c.commit(w.prepare_commit())
    w.close()
    c.close()


schema = pa.schema([("id", pa.int64()), ("s", row)])
catalog.create_table("default.t", Schema.from_pyarrow_schema(schema, options={"file.format": "parquet"}), False)
write([{"id": 1, "s": {"a": 10, "b": 100}}, {"id": 2, "s": {"a": 20, "b": None}}], schema)
catalog.alter_table("default.t", [SchemaChange.rename_column("s", "r")])
write([{"id": 3, "r": {"a": 30, "b": 300}}], pa.schema([("id", pa.int64()), ("r", row)]))
```
