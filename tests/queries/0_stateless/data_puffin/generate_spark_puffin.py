#!/usr/bin/env python3
"""Generate a Puffin deletion-vector file using Apache Spark + Iceberg v3.

Requirements:
  - Java 11+
  - pyspark
  - Network access on first run (downloads iceberg-spark-runtime)

Usage:
  python3 generate_spark_puffin.py
  python3 generate_puffin_fixtures.py
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError as exc:  # pragma: no cover - helper script
    raise SystemExit("pyspark is required") from exc

OUTPUT = Path(__file__).with_name("spark_deletion_vector.puffin")
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
DELETED_IDS = [2, 5, 7, 100, 65536]


def find_puffin_file(warehouse: Path) -> Path:
    puffin_files = sorted(warehouse.rglob("*-deletes.puffin"))
    if len(puffin_files) != 1:
        raise RuntimeError(f"Expected exactly one puffin file, found {len(puffin_files)}: {puffin_files}")
    return puffin_files[0]


def build_spark_session(warehouse: Path) -> SparkSession:
    return (
        SparkSession.builder.appName("generate_spark_puffin")
        .master("local[1]")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", str(warehouse))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def generate_puffin(warehouse: Path) -> Path:
    spark = build_spark_session(warehouse)
    try:
        spark.sql(
            """
            CREATE TABLE default.spark_puffin_source (id BIGINT)
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '3',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql("INSERT INTO default.spark_puffin_source SELECT id FROM range(0, 70000)")
        spark.sql(
            "DELETE FROM default.spark_puffin_source "
            f"WHERE id IN ({', '.join(str(x) for x in DELETED_IDS)})"
        )
    finally:
        spark.stop()

    return find_puffin_file(warehouse)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="clickhouse_spark_puffin_") as tmp:
        warehouse = Path(tmp)
        puffin_path = generate_puffin(warehouse)
        shutil.copyfile(puffin_path, OUTPUT)
        print(f"Wrote {OUTPUT} ({OUTPUT.stat().st_size} bytes) from {puffin_path}")


if __name__ == "__main__":
    main()
