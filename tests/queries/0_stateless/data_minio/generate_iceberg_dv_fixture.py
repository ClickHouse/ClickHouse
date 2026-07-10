#!/usr/bin/env python3
"""Generate an Iceberg v3 table fixture with Puffin deletion vectors for ClickHouse tests.

Requirements:
  - Java 11+
  - pyspark
  - Network access on first run (downloads iceberg-spark-runtime)

Usage:
  python3 generate_iceberg_dv_fixture.py [warehouse_dir]
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError as exc:  # pragma: no cover - helper script
    raise SystemExit("pyspark is required") from exc

DEFAULT_WAREHOUSE = Path(__file__).resolve().parent / "dv_puffin_warehouse"
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
TABLE_NAME = "dv_puffin_source"
DELETED_IDS = [2, 5, 7, 100]


def build_spark_session(warehouse: Path) -> SparkSession:
    return (
        SparkSession.builder.appName("generate_iceberg_dv_fixture")
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


def generate_fixture(warehouse: Path) -> Path:
    spark = build_spark_session(warehouse)
    try:
        spark.sql(
            f"""
            CREATE TABLE default.{TABLE_NAME} (id BIGINT)
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '3',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql(f"INSERT INTO default.{TABLE_NAME} SELECT id FROM range(0, 200)")
        spark.sql(
            f"DELETE FROM default.{TABLE_NAME} "
            f"WHERE id IN ({', '.join(str(x) for x in DELETED_IDS)})"
        )
    finally:
        spark.stop()

    table_dir = warehouse / "default" / TABLE_NAME
    if not table_dir.exists():
        raise RuntimeError(f"Expected table directory at {table_dir}")

    for crc_file in table_dir.rglob("*.crc"):
        crc_file.unlink()

    return table_dir


def main() -> None:
    warehouse = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_WAREHOUSE.resolve()
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)

    table_dir = generate_fixture(warehouse)
    print(f"Wrote Iceberg v3 deletion vector fixture to {table_dir}")


if __name__ == "__main__":
    main()
