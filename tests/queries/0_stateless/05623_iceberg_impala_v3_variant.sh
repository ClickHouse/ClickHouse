#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Format version 3 Iceberg tables with `variant` columns in Parquet, ORC and Avro data files, written by Trino and Spark,
# from the Apache Impala test data, see `data_iceberg/impala/README.md`. The expected results are the ones of the Impala
# tests `iceberg-v3-variant.test`, `iceberg-v3-variant-get.test` and `iceberg-v3-variant-spark.test` (for ORC and Avro,
# which Impala does not read, the values decoded from the data files). Variant values are read as `Dynamic` and
# compared as JSON. Values that have no JSON representation in Impala (binary, time, timestamptz, uuid), and the
# distinction between a variant null and SQL NULL, are not checked.
#
# Bugs, until they are fixed:
# - `iceberg_v3_spark_variant` has `location` `file:///test-warehouse/...` and the manifest list `file:/test-warehouse/...`,
#   the manifest list is not found: https://github.com/ClickHouse/ClickHouse/issues/129990
# - Variant in Avro data files: https://github.com/ClickHouse/ClickHouse/issues/129998
# - `trino_variant` has manifests without `partition-spec` in the Avro header: https://github.com/ClickHouse/ClickHouse/issues/129989
# - Variant values in ORC data files are read as NULL: https://github.com/ClickHouse/ClickHouse/issues/130031
# - Reading only the subcolumn `arr_struct_v.v` (a variant inside an array of structs) returns empty arrays:
#   https://github.com/ClickHouse/ClickHouse/issues/130023

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
-- Primitives, strings with escaping, arrays, nested objects, decimals, dates and timestamps, written by Trino.
SELECT 'trino_variant';
SELECT count(), count(v) FROM icebergLocal('iceberg_v3/trino_variant') WHERE id > 1;
SELECT id, toJSONString(v) FROM icebergLocal('iceberg_v3/trino_variant')
WHERE id BETWEEN 2 AND 39 OR id = 41 OR id BETWEEN 50 AND 56 OR id IN (59, 60) ORDER BY id;
SELECT JSONExtractString(toJSONString(v), 'name'), JSONExtractInt(toJSONString(v), 'age') FROM icebergLocal('iceberg_v3/trino_variant') WHERE id = 31;
SELECT JSONExtractString(toJSONString(v), 'context', 'page'), JSONExtractInt(toJSONString(v), 'int_array', 1) FROM icebergLocal('iceberg_v3/trino_variant') WHERE id = 35;
SELECT id FROM icebergLocal('iceberg_v3/trino_variant') WHERE JSONExtractString(toJSONString(v), 'name') = 'Alice' ORDER BY id;

-- Variant values inside arrays, maps and structs.
SELECT 'trino_nested_variant';
SELECT id, descr, toJSONString(top_v), toJSONString(arr_v), toJSONString(map_v), toJSONString(struct_v.a), struct_v.b, toJSONString(arr_struct_v.v)
FROM icebergLocal('iceberg_v3/trino_nested_variant') ORDER BY id;

-- Written by Spark, including long strings and an object with 300 fields.
SELECT 'iceberg_v3_spark_variant';
SELECT count(), count(v) FROM icebergLocal('iceberg_v3/iceberg_v3_spark_variant');
SELECT id, toJSONString(v) FROM icebergLocal('iceberg_v3/iceberg_v3_spark_variant') WHERE id IN (1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 16, 20, 21, 22) ORDER BY id;
SELECT id, JSONExtractInt(toJSONString(v), 'a'), length(JSONExtractString(toJSONString(v), 'b')) FROM icebergLocal('iceberg_v3/iceberg_v3_spark_variant') WHERE id IN (9, 10) ORDER BY id;
SELECT JSONExtractInt(toJSONString(v), 'f000'), JSONExtractInt(toJSONString(v), 'f150'), JSONExtractInt(toJSONString(v), 'f299'), length(JSONExtractKeys(toJSONString(v)))
FROM icebergLocal('iceberg_v3/iceberg_v3_spark_variant') WHERE id = 11;
SELECT JSONExtractString(toJSONString(v), 's'), JSONExtractInt(toJSONString(v), 'i'), JSONExtractBool(toJSONString(v), 'b') FROM icebergLocal('iceberg_v3/iceberg_v3_spark_variant') WHERE id = 8;
SELECT JSONExtractInt(toJSONString(v), 'a'), JSONExtractString(toJSONString(v), 'd') FROM icebergLocal('iceberg_v3/iceberg_v3_spark_variant') WHERE id = 18;
SELECT count() FROM icebergLocal('iceberg_v3/iceberg_v3_spark_variant') WHERE JSONExtractInt(toJSONString(v), 'a') = 1;

-- The same values in Parquet, ORC and Avro data files.
SELECT 'iceberg_v3_variant_orc';
SELECT id, p, toJSONString(v) FROM icebergLocal('iceberg_v3/iceberg_v3_variant_orc') ORDER BY id;
SELECT 'iceberg_v3_variant_avro';
SELECT id, p, toJSONString(v) FROM icebergLocal('iceberg_v3/iceberg_v3_variant_avro') ORDER BY id;
SELECT 'iceberg_v3_variant_mixed';
SELECT id, p, toJSONString(v) FROM icebergLocal('iceberg_v3/iceberg_v3_variant_mixed') WHERE p = 1 ORDER BY id;
SELECT id, p, toJSONString(v) FROM icebergLocal('iceberg_v3/iceberg_v3_variant_mixed') ORDER BY id;
" 2>&1
