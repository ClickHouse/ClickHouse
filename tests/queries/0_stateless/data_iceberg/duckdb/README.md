# Iceberg tables from the DuckDB Iceberg extension tests

The tables in this directory are copied from `data/persistent` of the DuckDB Iceberg extension,
https://github.com/duckdb/duckdb-iceberg/tree/b57f45c2c78965c71f699aa23ae73c292923668e/data/persistent
(MIT License, see the license text below). The expected results in the tests `05620_iceberg_duckdb_*` to
`05625_iceberg_duckdb_*` are taken from the sqllogictest files of the extension, `test/sql/local/**/*.test`
at the same commit, and from the comments in them that describe how the tables were written.
The tests `05572_parquet_map_requested_as_tuple` (table `mapping_failures/map_to_struct`) and
`05612_iceberg_row_lineage_stored_values_pruning` (table `row_lineage_stored_columns`) use the same tables.

The directory layout `data/persistent/<table>` is kept, because most tables have relative locations like
`data/persistent/<table>`, and some manifest lists reference files of other tables
(`deletion_vector_bitmap_count` uses the table `legacy_bare_deletion_vector`, `manifest_list_invalid_spec_id`
uses the manifests of `partition_bool`, `manifest_newer_than_table` uses a manifest of `partition_float`).
The tests run the queries from this directory. The names of the metadata files are kept as they are, including
the non-standard ones (`vfinal.metadata.json`, `final.metadata.json`, `spec_id_999.metadata.json`,
`no-snapshot-schema-id.metadata.json` and so on), they are selected with `iceberg_metadata_file_path`.
No file is modified.

Changes compared to the originals:

- `special_paths.tar` contains the tables whose file names contain `%` (escaped partition values):
  `partition_fixed_length_binary`, `partition_time`, `partition_timestamp`, `partition_timestamptz`,
  `partition_timestamptz_ns`. The test extracts it into a temporary directory.
- The SQLite catalogs of PyIceberg (`pyiceberg_catalog.db`, `catalog.db`, `catalog.sqlite`) are not included.

Tables of the DuckDB test data that are not included:

- `benchmark`: data for benchmarks, not for tests.
- `iceberg/lineitem_iceberg`, `iceberg/lineitem_iceberg_gz`, `iceberg/lineitem_iceberg_gz_no_hint`,
  `iceberg/lineitem_iceberg_no_hint`: copies of the TPC-H lineitem table of about 2.5 MB each, too large to keep
  in the repository. Time travel is covered by other Iceberg tests.
- `encrypted_table`: tables encrypted with Iceberg table encryption, which ClickHouse does not support.
- `big_query_error`: written by BigQuery, the DuckDB test is skipped (the Avro schema of the manifest list
  has no field ids and nullable required fields).
- `generated_bounds`: the data files are not included upstream (`s3://fake-bucket/...`), the test checks
  only that all data files are skipped by their bounds.
- `utf8_trunc_bounds`: the bounds in the manifest do not match the data, the expected result depends on
  skipping a data file by its bounds.
- `snapshot_refs`: metadata only, used for a DuckDB function that lists the references.
- `missing_map_keys/missing_value`: the value of a map has a field id that the table never assigned.
  DuckDB reads it as NULL, ClickHouse refuses the data file with `ICEBERG_SPECIFICATION_VIOLATION`.
- `bad_data`: not an Iceberg table.

## License

```
Copyright 2018-2025 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
