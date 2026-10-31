## Paimon tables with `DATE` columns

Four tables of one Paimon warehouse, written by the program below with `pypaimon==2.1.0`
(`pip install pypaimon==2.1.0`, no JVM needed) and copied from `<warehouse>/default.db/`.
They are reachable as `paimonS3(s3_conn, filename='paimon_date32/<table>')`.

* `t_parquet`: nullable `DATE`, `DATE NOT NULL` and `ARRAY<DATE>`, with the `Date32` (and Paimon `DATE`) bounds
  `0000-01-01` and `9999-12-31`, the `Date` bounds `1970-01-01` and `2149-06-06`, and the days just outside the
  `Date` range.
* `t_partitioned`: partitioned by a `DATE` column: `d=-1`, `d=19723` and `d=84006`.
* `t_orc`, `t_avro`: the rows of `t_partitioned`, unpartitioned, in the ORC and Avro file formats, read with
  `format = 'ORC'` and `format = 'Avro'`.

```python
# Usage: python3 gen_paimon_date32.py <warehouse_dir>   (pip install pypaimon==2.1.0)
import sys
from datetime import date

import pyarrow as pa
from pypaimon import CatalogFactory, Schema

catalog = CatalogFactory.create({"warehouse": sys.argv[1]})
catalog.create_database("default", True)


def write(name, schema, rows, partition_keys=None, file_format="parquet"):
    catalog.create_table(
        f"default.{name}",
        Schema.from_pyarrow_schema(schema, partition_keys=partition_keys, options={"file.format": file_format}),
        False,
    )
    wb = catalog.get_table(f"default.{name}").new_batch_write_builder()
    w, c = wb.new_write(), wb.new_commit()
    w.write_arrow(pa.Table.from_pylist(rows, schema=schema))
    c.commit(w.prepare_commit())
    w.close()
    c.close()


# Date32 and Paimon DATE cover 0000-01-01 (-719528) .. 9999-12-31 (2932896); Date covers 1970-01-01 (0) .. 2149-06-06 (65535).
dates = [-719528, -1, 0, 65535, 65536, 2932896]
other = 19723  # 2024-01-01

write(
    "t_parquet",
    pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("d", pa.date32()),
        pa.field("d_nn", pa.date32(), nullable=False),
        pa.field("ad", pa.list_(pa.date32())),
    ]),
    [{"id": i + 1, "d": d, "d_nn": d, "ad": [d, other]} for i, d in enumerate(dates)]
    + [{"id": 7, "d": None, "d_nn": other, "ad": [None]}],
)

simple = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("d", pa.date32())])
simple_rows = [{"id": 1, "d": date(1969, 12, 31)}, {"id": 2, "d": other}, {"id": 3, "d": date(2200, 1, 1)}]
write("t_partitioned", simple, simple_rows, partition_keys=["d"])
write("t_orc", simple, simple_rows, file_format="orc")
write("t_avro", simple, simple_rows, file_format="avro")
```
