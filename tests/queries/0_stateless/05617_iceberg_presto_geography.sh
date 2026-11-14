#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and Avro are not in fast test

# Iceberg v3 `geography` and `geometry` columns in Parquet files written by PyArrow (GeoArrow) and
# parquet-java, wrapped in v3 metadata by the Iceberg Java library. The tables and the expected
# results come from the Presto native tests (`TestIcebergV3GeographyImportedRead`), see
# `data_iceberg/presto/README.md`. Every table has `id`, and the `geography_arrow_*` tables also
# `source` (the well-known text of `geog`), `geog` and `geogs` (`[geog, NULL]`); the row with `id = 9`
# is all nulls.
#
# The files of `geography_arrow_logical`, `geography_arrow_plain` and `geography_arrow_null_file`
# contain a `GEOMETRYCOLLECTION`, so ClickHouse cannot read them (`Incorrect geometry type 7`) until
# https://github.com/ClickHouse/ClickHouse/issues/129999 is fixed. The well-known text of the
# collection is not checked, because its representation in `Geometry` is not decided yet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/presto"

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`, so the tables
# are copied into a uniquely named subdirectory of the working directory.
WORK="./${CLICKHOUSE_TEST_UNIQUE_NAME}_presto"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT
WORK=$(realpath "$WORK")

# Presto's tests replace the `FILEPATH` placeholder in the metadata with the absolute path of
# a copy of the table; do the same, see `substitute_filepath.py`.
for table in geography_arrow_logical geography_arrow_plain geography_arrow_null_file geography_arrow_crs geography_parquet_java_karney geometry_arrow
do
    python3 -I "$DATA/substitute_filepath.py" "$DATA/presto-native-tests/$table" "$WORK/$table"
done

# The `GEOGRAPHY` logical type in Parquet (`logical`) and an unannotated `binary` column (`plain`),
# which only the Iceberg schema identifies as a geography.
for table in geography_arrow_logical geography_arrow_plain
do
    $CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 -q "
    SELECT '$table';
    DESCRIBE icebergLocal('$WORK/$table');
    SELECT count(), countIf(geog IS NOT NULL) FROM icebergLocal('$WORK/$table');
    -- Every value except the collection matches the geography built from its source text,
    -- and the array holds the same value followed by a null.
    SELECT id, variantType(geog), wkt(geog), wkt(geog) = wkt(readWKT(source)), length(geogs), wkt(geogs[1]) = wkt(geog), geogs[2] IS NULL
    FROM icebergLocal('$WORK/$table') WHERE source NOT LIKE 'GEOMETRYCOLLECTION%' ORDER BY id;
    SELECT id, geog IS NULL, source IS NULL, length(geogs) FROM icebergLocal('$WORK/$table') WHERE geog IS NULL ORDER BY id;
    "
done

# The data file of `geography_arrow_logical` and a second file in which every column but `id` is null.
$CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 -q "
SELECT 'geography_arrow_null_file';
SELECT id, geog IS NULL, length(geogs) FROM icebergLocal('$WORK/geography_arrow_null_file') WHERE id >= 9 ORDER BY id;
SELECT id FROM icebergLocal('$WORK/geography_arrow_null_file') WHERE geog IS NULL ORDER BY id;
SELECT count() FROM icebergLocal('$WORK/geography_arrow_null_file') WHERE geog IS NOT NULL;
SELECT id, wkt(geog), wkt(geogs[1]), length(geogs) FROM icebergLocal('$WORK/geography_arrow_null_file')
WHERE source IS NULL OR source NOT LIKE 'GEOMETRYCOLLECTION%' ORDER BY id;
"

# Presto rejects these two tables, because it supports only the WGS84 CRS and spherical edges.
# ClickHouse does not interpret the CRS and the edge interpolation algorithm and reads the values.
# The values are checked against the WKB in the data files.
$CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 -q "
SELECT 'geography_arrow_crs';
DESCRIBE icebergLocal('$WORK/geography_arrow_crs');
SELECT id, wkt(geog) FROM icebergLocal('$WORK/geography_arrow_crs') ORDER BY id;
SELECT 'geography_parquet_java_karney';
DESCRIBE icebergLocal('$WORK/geography_parquet_java_karney');
SELECT id, wkt(geog) FROM icebergLocal('$WORK/geography_parquet_java_karney') ORDER BY id;
"

# A `geometry` column with the Parquet `GEOMETRY` logical type.
$CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 -q "
SELECT 'geometry_arrow';
DESCRIBE icebergLocal('$WORK/geometry_arrow');
SELECT id FROM icebergLocal('$WORK/geometry_arrow') WHERE geom IS NULL;
SELECT id, wkt(geom) FROM icebergLocal('$WORK/geometry_arrow') WHERE geom IS NOT NULL ORDER BY id;
"
