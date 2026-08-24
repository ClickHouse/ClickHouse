#!/usr/bin/env python3
"""Generate Iceberg v3 table fixtures with Puffin deletion vectors for ClickHouse tests.

Requirements:
  - Java 11+
  - pyspark
  - Network access on first run (downloads iceberg-spark-runtime)

Usage:
  python3 generate_iceberg_dv_fixture.py [warehouse_dir]
  python3 generate_iceberg_dv_fixture.py --complex-only [warehouse_dir]
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
SIMPLE_TABLE_NAME = "dv_puffin_source"
COMPLEX_TABLE_NAME = "dv_puffin_complex"
SIMPLE_DELETED_IDS = [2, 5, 7, 100]
COMPLEX_DELETED_IDS = [205, 210, 220]


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


def generate_simple_fixture(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE default.{SIMPLE_TABLE_NAME} (id BIGINT)
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO default.{SIMPLE_TABLE_NAME} SELECT id FROM range(0, 200)")
    spark.sql(
        f"DELETE FROM default.{SIMPLE_TABLE_NAME} "
        f"WHERE id IN ({', '.join(str(x) for x in SIMPLE_DELETED_IDS)})"
    )


def generate_complex_fixture(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE default.{COMPLEX_TABLE_NAME} (id BIGINT, data STRING)
        USING iceberg
        PARTITIONED BY (bucket(5, id))
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"INSERT INTO default.{COMPLEX_TABLE_NAME} "
        f"SELECT id, char(id + ascii('a')) FROM range(10, 100)"
    )
    spark.sql(f"DELETE FROM default.{COMPLEX_TABLE_NAME} WHERE id < 20")
    spark.sql(f"DELETE FROM default.{COMPLEX_TABLE_NAME} WHERE id >= 90")
    spark.sql(
        f"INSERT INTO default.{COMPLEX_TABLE_NAME} "
        f"SELECT id, char(id + ascii('a')) FROM range(100, 200)"
    )
    spark.sql(f"DELETE FROM default.{COMPLEX_TABLE_NAME} WHERE id >= 150")
    spark.sql(f"ALTER TABLE default.{COMPLEX_TABLE_NAME} ADD COLUMNS (label STRING)")
    spark.sql(
        f"""
        INSERT INTO default.{COMPLEX_TABLE_NAME}
        SELECT id, char(id + ascii('a')), 'new'
        FROM range(200, 250)
        """
    )
    spark.sql(
        f"DELETE FROM default.{COMPLEX_TABLE_NAME} "
        f"WHERE id IN ({', '.join(str(x) for x in COMPLEX_DELETED_IDS)})"
    )
    spark.sql(f"UPDATE default.{COMPLEX_TABLE_NAME} SET label = 'updated' WHERE id = 25")
    spark.sql(f"CALL system.rewrite_data_files(table => 'default.{COMPLEX_TABLE_NAME}')")


def cleanup_crc_files(table_dir: Path) -> None:
    for crc_file in table_dir.rglob("*.crc"):
        crc_file.unlink()


def generate_fixture(warehouse: Path, *, simple: bool, complex_table: bool) -> None:
    spark = build_spark_session(warehouse)
    try:
        if simple:
            generate_simple_fixture(spark)
        if complex_table:
            generate_complex_fixture(spark)
    finally:
        spark.stop()

    if simple:
        simple_dir = warehouse / "default" / SIMPLE_TABLE_NAME
        if not simple_dir.exists():
            raise RuntimeError(f"Expected table directory at {simple_dir}")
        cleanup_crc_files(simple_dir)

    if complex_table:
        complex_dir = warehouse / "default" / COMPLEX_TABLE_NAME
        if not complex_dir.exists():
            raise RuntimeError(f"Expected table directory at {complex_dir}")
        cleanup_crc_files(complex_dir)


def main() -> None:
    args = sys.argv[1:]
    complex_only = False
    if args and args[0] == "--complex-only":
        complex_only = True
        args = args[1:]

    warehouse = Path(args[0]).resolve() if args else DEFAULT_WAREHOUSE.resolve()
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)

    generate_fixture(warehouse, simple=not complex_only, complex_table=True)

    if complex_only:
        print(f"Wrote complex Iceberg v3 deletion vector fixture to {warehouse / 'default' / COMPLEX_TABLE_NAME}")
    else:
        print(f"Wrote Iceberg v3 deletion vector fixtures to {warehouse / 'default'}")


if __name__ == "__main__":
    main()
