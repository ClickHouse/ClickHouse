# Iceberg tables from the Apache Impala test data

The tables in this directory are copied from the Apache Impala test data, `testdata/data/iceberg_test` at commit
https://github.com/apache/impala/tree/e35eb8f3582449d736e74ce32f1138f5f5dc085b/testdata/data/iceberg_test
(Apache License 2.0, https://github.com/apache/impala/blob/master/LICENSE.txt).

The files are not modified, and the directory layout is kept, because the table metadata refers to the other
tables' locations relative to `/test-warehouse/iceberg_test/` (for example, `iceberg_multiple_storage_locations`
has its data files in the sibling directories `iceberg_multiple_storage_locations_data*`). The tables were written
by Spark, Hive, Flink, NiFi, Trino and Impala itself; `testdata/data/README` in the Impala repository describes how
some of them were made. The expected results in the tests are taken from the Impala tests in
`testdata/workloads/functional-query/queries/QueryTest/iceberg-*.test` and `tests/query_test/test_iceberg.py`
of the same commit. For the values that Impala does not read (`uuid` and `variant` in ORC and Avro, `binary` and
`timestamptz` initial defaults, tables that the Impala tests only read after modifying them), the expected results
are taken from PyIceberg, from the data files decoded directly, or from the table metadata.

The metadata is as checked into the Impala repository. Note what Impala's test data loader
(`testdata/bin/load-test-warehouse-snapshot.sh`, `tests/util/iceberg_metadata_util.py`) does with it:

- It rewrites the location prefix only when the warehouse is not on HDFS; the checked-in locations are
  `/test-warehouse/...` (resolved against the default file system `hdfs://localhost:20500`), `file:///...` and
  `file:/...`. The position delete files refer to the data files as `hdfs://localhost:20500/test-warehouse/...`.
- The Avro manifests and manifest lists were rewritten by that script with a plain Avro writer, so they don't have
  the Iceberg keys (`partition-spec`, `schema`, `format-version`, `content`) in the Avro header.

Tables of the Impala test data that are not included:

- `hadoop_catalog/ice/iceberg_v2_null_delete_record`: position delete files with NULL in `file_path`, which is a
  required field. Impala skips such records with a warning; it is not clear that ClickHouse should do the same.
- `hadoop_catalog/ice/iceberg_v2_delete_equality_partition_evolution`: equality deletes in a table with several
  partition specs. Impala refuses to read it, so there are no expected results.
