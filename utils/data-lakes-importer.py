#!/usr/bin/env python3

import os
import sys

import pyspark
from delta import *  # pip install delta-spark

# Usage example:
# ./data-lakes-importer.py iceberg data.parquet result_path


def get_spark_for_iceberg(result_path):
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", result_path)
        .master("local")
    )
    return builder.master("local").getOrCreate()


def get_spark_for_delta():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local")
    )

    return configure_spark_with_delta_pip(builder).master("local").getOrCreate()


def get_spark_for_hudi():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "spark.jars.packages",
            "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0",
        )
        .config(
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.catalog.local", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
        )
        .config(
            "spark.driver.memory", "20g"
        )  # .config('spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
        .master("local")
    )
    return builder.master("local").getOrCreate()


def main():
    data_lake_name = str(sys.argv[1]).strip()
    file_path = sys.argv[2]
    result_path = sys.argv[3]

    if not file_path.startswith("/"):
        print(f"Expected absolute path, got relative: {file_path}")
        exit(1)

    if not result_path.startswith("/"):
        print(f"Expected absolute path, got relative: {result_path}")
        exit(1)

    spark = None
    if data_lake_name == "iceberg":
        spark = get_spark_for_iceberg(result_path)
        spark.conf.set("spark.sql.debug.maxToStringFields", 100000)
        spark.read.load(f"file://{file_path}").writeTo("iceberg_table").using(
            "iceberg"
        ).create()
    elif data_lake_name == "delta":
        spark = get_spark_for_delta()
        spark.conf.set("spark.sql.debug.maxToStringFields", 100000)
        spark.read.load(f"file://{file_path}").write.mode("overwrite").option(
            "compression", "none"
        ).format("delta").option("delta.columnMapping.mode", "name").save(result_path)
    elif data_lake_name == "hudi":
        spark = get_spark_for_hudi()
        spark.conf.set("spark.sql.debug.maxToStringFields", 100000)
        spark.read.load(f"file://{file_path}").write.mode("overwrite").option(
            "compression", "none"
        ).format("hudi").option("hoodie.table.name", "hudi").option(
            "hoodie.datasource.write.partitionpath.field", "partitionpath"
        ).option(
            "hoodie.datasource.write.table.name", "hudi"
        ).option(
            "hoodie.datasource.write.recordkey.field", "ts"
        ).option(
            "hoodie.datasource.write.precombine.field", "ts"
        ).option(
            "hoodie.datasource.write.operation", "insert_overwrite"
        ).save(
            result_path
        )
    else:
        print(
            f"Unknown data lake name {data_lake_name}. Support only: 'iceberg', 'delta'"
        )
        exit(1)


if __name__ == "__main__":
    main()
