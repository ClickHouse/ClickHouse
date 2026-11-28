# Iceberg v3 tables from the Presto test suite

The tables in this directory are copied without changes from the Presto test resources at commit
https://github.com/prestodb/presto/tree/c9613ccc690c57bb373dffb03ab721fc64cac772
(Apache License 2.0, https://github.com/prestodb/presto/blob/master/LICENSE):

- `presto-iceberg/` - `presto-iceberg/src/test/resources/iceberg_v3/`: `geometry_data_type_read`,
  `geometry_array`, `geometry_map`, `geometry_row` (`geometry` at the top level and nested in
  `array`, `map` and `struct`) and `variant_type_read` (`variant`, written by Spark 4.1 with Iceberg 1.10.1).
- `presto-native-tests/` - `presto-native-tests/src/test/resources/iceberg_v3/`: `geography_arrow_logical`,
  `geography_arrow_plain`, `geography_arrow_null_file`, `geography_arrow_crs` (Parquet files written
  by PyArrow, GeoArrow WKB with and without the Parquet `GEOGRAPHY` logical type, a non-default CRS),
  `geography_parquet_java_karney` (parquet-java, the `karney` edge interpolation algorithm) and
  `geometry_arrow`.

This is the whole content of both directories; nothing is left out.

The expected results in the tests are taken from the Presto tests `TestGeometryTypeDataRead`,
`TestGeometryArrayRead`, `TestGeometryMapRead`, `TestGeometryRowRead`, `TestIcebergVariantTypeRead`
(`presto-iceberg/src/test/java/com/facebook/presto/iceberg/`) and `TestIcebergV3GeographyImportedRead`
(`presto-native-tests/src/test/java/com/facebook/presto/nativetests/iceberg/`). Presto rejects
`geography_arrow_crs` and `geography_parquet_java_karney`, because it supports only WGS84 with
spherical edges; ClickHouse does not interpret the CRS and the algorithm, and the expected values
of these two tables are the WKB values in their data files.

## The `FILEPATH` placeholder

The metadata of these tables stores every path with the placeholder `FILEPATH` in place of the
table location (`file://FILEPATH/...`, or `FILEPATH/...` in `variant_type_read`), in the
`.metadata.json` files and in the records of the Avro manifest lists and manifests (some of them
`deflate`-compressed). Presto's tests (`IcebergImportedTableTestBase.setupAndRegisterTable`) copy
a table into a temporary directory and replace `FILEPATH` with the absolute path of the copy in
every `.json` and `.avro` file of `metadata/` before registering it.

The tests do the same at run time with `substitute_filepath.py`, which copies a table into a
temporary directory and decodes and re-encodes the Avro records with the substituted strings. The
files in this directory are kept as they are, so that they can be compared with upstream, and
because the substituted path depends on where the test runs. Unlike Presto, which re-writes the
Avro files with `DataFileWriter` and so drops the Avro header metadata and the codec,
`substitute_filepath.py` keeps the header and the codec as they are. The manifests of the tables in
`presto-iceberg/` already have no `partition-spec`, `partition-spec-id` and `schema` header keys
upstream (only `avro.schema` and `avro.codec`).
