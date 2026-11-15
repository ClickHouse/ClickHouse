#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# GeoParquet 1.1 native (GeoArrow) encodings: `point`, `linestring`, `polygon`, `multipoint`,
# `multilinestring`, `multipolygon`. The files were written by pyarrow, the second row has NULLs.

PARQUET="$CUR_DIR/data_parquet/05497_geoparquet_native_encodings.parquet"
ARROW="$CUR_DIR/data_arrow/05497_geoparquet_native_encodings.arrow"

echo "--- Parquet: schema inference"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$PARQUET')"

echo "--- Parquet: data"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$PARQUET') ORDER BY id FORMAT Vertical"

echo "--- Parquet: geo functions"
$CLICKHOUSE_LOCAL -q "SELECT id, areaCartesian(polygon), areaCartesian(multipolygon), lengthCartesian(linestring), pointInPolygon(point, polygon) FROM file('$PARQUET') ORDER BY id"

echo "--- Parquet: a subset of columns"
$CLICKHOUSE_LOCAL -q "SELECT multipolygon, id FROM file('$PARQUET') ORDER BY id"

echo "--- Parquet: explicit Geometry type"
$CLICKHOUSE_LOCAL -q "SELECT id, variantType(point), point, variantType(polygon), polygon FROM file('$PARQUET', Parquet, 'id Int32, point Geometry, polygon Geometry') ORDER BY id"

echo "--- Parquet: case-insensitive column matching"
$CLICKHOUSE_LOCAL -q "SELECT ID, variantType(POINT), POINT, POLYGON FROM file('$PARQUET', Parquet, 'ID Int32, POINT Geometry, POLYGON Polygon') ORDER BY ID SETTINGS input_format_parquet_case_insensitive_column_matching = 1"

echo "--- Parquet: the parser of GeoParquet is disabled"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(point), toTypeName(polygon) FROM file('$PARQUET') LIMIT 1 SETTINGS input_format_parquet_allow_geoparquet_parser = 0"

echo "--- Arrow: schema inference"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$ARROW')"

echo "--- Arrow: data"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$ARROW') ORDER BY id FORMAT Vertical"

echo "--- Arrow: explicit Geometry type"
$CLICKHOUSE_LOCAL -q "SELECT id, variantType(point), point, variantType(polygon), polygon FROM file('$ARROW', Arrow, 'id Int32, point Geometry, polygon Geometry') ORDER BY id"

echo "--- Arrow: case-insensitive column matching"
$CLICKHOUSE_LOCAL -q "SELECT ID, variantType(POINT), POINT, POLYGON FROM file('$ARROW', Arrow, 'ID Int32, POINT Geometry, POLYGON Polygon') ORDER BY ID SETTINGS input_format_arrow_case_insensitive_column_matching = 1"

# The row with NULLs is excluded, because an empty `Polygon` is written as WKB of a polygon with one empty ring.
echo "--- Round trip through Parquet with WKB"
$CLICKHOUSE_LOCAL -q "
    SELECT * FROM file('$PARQUET') WHERE id != 2 ORDER BY id INTO OUTFILE '${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_wkb.parquet' TRUNCATE FORMAT Parquet;
    SELECT count() FROM (
        SELECT * FROM file('${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_wkb.parquet')
        EXCEPT
        SELECT * FROM file('$PARQUET'));
    SELECT count() FROM file('${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_wkb.parquet');
"
rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_wkb.parquet"
