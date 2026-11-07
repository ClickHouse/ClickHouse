# Iceberg tables from the iceberg-cxx test suite

The tables in this directory are copied from the test resources of lithium-tech/iceberg-cxx at commit
https://github.com/lithium-tech/iceberg-cxx/tree/4ff52e71673393b0d059f5d066a8bc7b333be85a/tests
(Apache License 2.0, https://github.com/lithium-tech/iceberg-cxx/blob/main/LICENSE):
`tests/tables/*` and `tests/warehouse/streams/default_value`. The files are not modified.

The tables are written by Trino 408-480 (`create.sql` next to a table has the statements that generated
it and the engine version) and by Spark 3.5 (`prod/db/refdeletes3`, `types/all_spark_types`,
`types/decimals`, `types/unsupported_types`, `streams/default_value`), except `v_20240913`, written by
an unknown tool with the data files outside the table location. The table locations are
`s3a://warehouse/<table>` or `s3://warehouse/<table>`; ClickHouse maps them to the local directory.

The expected results in the tests are taken from the `INSERT`/`DELETE` statements in `create.sql`,
from the upstream gtest asserts (`tests/streams_test.cpp`, `tests/tea_scan_test.cpp`), and, for the
Spark tables without a generation script and for `v_20240913`, from PyIceberg and DuckDB.

Changes compared to the originals:

- `identity_partitioning.tar` contains `tests/tables/identity_partitioning`, whose partition directory names
  contain `%` (escaped `:`, `+` and `=`). Tests extract it into a temporary directory.

Special tables:

- `v_20240913/iceberg`: the data files are outside the table location (in `v_20240913/` next to the
  `iceberg/` directory with the metadata).
- `streams/default_value/metadata/DefaultValues.json`: a copy of the latest metadata with `initial-default`
  values for the added columns, read with `iceberg_metadata_file_path`.

Not included:

- `tests/tables/partitioned_table_with_missing_spec`, `partitioned_table_with_multiple_spec`,
  `partitioned_table_with_void_transform`: only a modified metadata JSON that refers to the manifest lists and
  data files of `partitioned_table` through its location `s3a://warehouse/partitioned_table`; the upstream
  test maps the paths with a custom file system, which a table directory cannot express.
- `tests/meta_perf`: metadata-only performance fixtures without data files.
- `tests/metadata`, `tests/warehouse/*.json`: metadata-only fixtures of unit tests of the metadata parser.
- `tests/warehouse/deletion_vectors`, `tests/warehouse/gperov`, `tests/warehouse/empty`: duplicates of
  `tests/tables/deletion_vectors` and of the tables in `data_iceberg/tea`.
