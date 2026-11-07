# Iceberg tables from the Dremio test suite

The tables in this directory are copied from the Dremio test resources,
`sabot/kernel/src/test/resources/iceberg` at commit
https://github.com/dremio/dremio-oss/tree/799ccbda47e6f2e1bfacf1ccbded174e00d4150a/sabot/kernel/src/test/resources/iceberg
(Apache License 2.0, https://github.com/dremio/dremio-oss/blob/master/LICENSE).

The files are unchanged, except that the Hadoop checksum files (`.*.crc`) are not copied. The metadata
refers to the original locations of the tables: `/tmp/iceberg`, `file:///tmp/iceberg` (format version 1 tables)
and `/tmp/iceberg-test-tables/v2/...` (format version 2 tables), with manifest lists, manifests and data files
referenced as `/tmp/...`, `file:/tmp/...` or `file:///tmp/...`. Dremio copies the tables to these locations
before the tests (`IcebergTestTables`); ClickHouse should resolve the paths relative to the table location.

Tables:

- `empty_table`: format version 1, no snapshots, two versions of the first metadata file.
- `nation`: format version 1 (Apache Iceberg 0.x), 25 rows of TPC-H `nation`; the schema has no `schema-id`.
- `partitionednation`: format version 1, partitioned by identity of `n_regionkey`, which is renamed to `regionkey`
  in `v2.metadata.json`; Dremio data files without field ids; two more rows in two more snapshots;
  the first snapshot has no `schema-id`; `dremio-partitionStats*` files.
- `internal_metadata_inconsistent_partitions`: format version 1, partitioned by identity of `dir0` and `dir1`; one manifest
  has partition tuples with `dir0` only; the manifests have no `partition-spec` and `schema` keys in their Avro metadata.
- `table_with_delete`: format version 2, six partitions of 100 rows, a position delete file for six rows.
  `v2_table_files_out.csv` holds the expected file list of the Dremio test.
- `v2/orders`: format version 2, 600 orders, no deletes.
- `v2/multi_rowgroup_orders_with_deletes`: format version 2, 9000 orders in data files with several row groups,
  position deletes in three snapshots.
- `v2/products_with_eq_deletes`: format version 2, equality deletes on the key and on a non-key column, a position delete;
  `README.md` describes how it was built and the expected number of rows (450).
- `v2/products_with_eq_deletes_upper_case_columns`: the same table with upper case column names.
- `v2/Merge_On_Read_Target_Partitioned`, `v2/Merge_On_Read_Target_Unpartitioned`: one data file of 100000 rows each.

The expected results in the tests come from the Dremio tests (`TestIcebergScan`, `TestIcebergTableFilesFunction`,
`TestIcebergManifestListRecordReader`), the `README.md` of `products_with_eq_deletes`, and from an independent reader:
PyIceberg for the tables it can read, and the data and delete files, read according to the manifests, for the others
(PyIceberg does not support equality deletes, old manifest lists without row counts and data files without field ids).

Tables of the Dremio test resources that are not included:

- `v2/longDir.../multi_rowgroup_orders_with_deletes`: a copy of `v2/multi_rowgroup_orders_with_deletes` under three
  directories with names of 110 characters.
- `v2/Merge_On_Read_Source_Partitioned`: the source table of the same `MERGE` tests, the same layout as
  `v2/Merge_On_Read_Target_Partitioned` (1.7 MB).
- `supplier`, `orders`, `complex*`, `mapTest`, `root_pointer`, `non_iceberg_with_column_ids`, `orphan_files`: plain Parquet
  or JSON files, not Iceberg tables. `utils`: Iceberg view metadata files.
- `dac/backend/src/test/resources/datasets/iceberg`: `empty_table` is the same table; `metastore_table` contains only
  the metadata directory, without data files.
