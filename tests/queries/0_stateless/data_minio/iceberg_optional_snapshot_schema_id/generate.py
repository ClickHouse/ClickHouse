import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession


stage = sys.argv[1]
assert stage in ("legacy", "upgrade")
warehouse = Path("/tmp/clickhouse-iceberg-optional-snapshot-schema-id")
table = warehouse / "default" / "iceberg_optional_snapshot_schema_id"
if stage == "legacy" and table.exists():
    raise FileExistsError(table)

spark = (
    SparkSession.builder.master("local[1]")
    .appName("iceberg_optional_snapshot_schema_id")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", str(warehouse))
    .getOrCreate()
)
name = "local.default.iceberg_optional_snapshot_schema_id"

try:
    if stage == "legacy":
        spark.sql(
            f"CREATE TABLE {name} (c INT) USING iceberg "
            "TBLPROPERTIES ('format-version'='1', "
            "'write.parquet.compression-codec'='uncompressed')"
        )
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql(f"INSERT INTO {name} VALUES (2)")
        version = 3
    else:
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='2')")
        version = 4

    metadata = json.loads((table / "metadata" / f"v{version}.metadata.json").read_text())
    assert metadata["properties"]["owner"] == "clickhouse"
    assert len(metadata["snapshots"]) == 2
    assert all("schema-id" not in snapshot for snapshot in metadata["snapshots"])
    assert [row.c for row in spark.sql(f"SELECT c FROM {name} ORDER BY c").collect()] == [1, 2]
    print(json.dumps(metadata, indent=2))

    if stage == "upgrade":
        spark.sql(f"ALTER TABLE {name} ADD COLUMN extra STRING")
        spark.sql(f"INSERT INTO {name} VALUES (3, cast(null as string))")
        metadata = json.loads((table / "metadata" / "v5.metadata.json").read_text())
        assert metadata["current-schema-id"] == 1
        assert len(metadata["schemas"]) == 2
        assert len(metadata["snapshots"]) == 3
        assert sum("schema-id" in snapshot for snapshot in metadata["snapshots"]) == 1
        assert [tuple(row) for row in spark.sql(f"SELECT c, extra FROM {name} ORDER BY c").collect()] == [(1, None), (2, None), (3, None)]
        print(json.dumps(metadata, indent=2))
finally:
    spark.stop()
