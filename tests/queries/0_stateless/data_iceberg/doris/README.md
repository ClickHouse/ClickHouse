# Iceberg tables from the Apache Doris tests

The tables in this directory come from the Apache Doris project (https://github.com/apache/doris,
Apache License 2.0, https://github.com/apache/doris/blob/master/LICENSE.txt), commit
https://github.com/apache/doris/tree/41cd2b473e9d9bb26be089f302cdfdfe6e107eaf
(2026-11-07), from two places:

- `be/`: the tables of the Doris BE unit tests, `be/test/exec/test_data/*_iceberg_*` in the repository
  (https://github.com/apache/doris/tree/41cd2b473e9d9bb26be089f302cdfdfe6e107eaf/be/test/exec/test_data).
  They were written by Spark into HDFS (`hdfs://hdfs-cluster/user/hive/warehouse/cqtest.db/...`).
  The table location of `complex_user_profiles_iceberg_parquet` is `.../complex_user_profiles_iceberg`.
  Doris checks only the column structure of these tables, so the expected values in the tests are checked with PyIceberg,
  and the ORC and Parquet versions of each table give the same result.
- `warehouse/`: the tables of the Doris external table regression tests (`regression-test/suites/external_table_p0/iceberg`).
  They are not in the repository: the Doris test environment downloads the archive
  https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com/regression/datalake/pipeline_data/iceberg_data_spark40_paimon142.zip
  (downloaded on 2026-11-07) in `start_iceberg` of `docker/thirdparties/run-thirdparties-docker.sh` and unpacks it
  as the data directory of the Iceberg docker compose stack (`docker/thirdparties/docker-compose/iceberg`).
  The archive is a snapshot of the MinIO bucket `warehouse` of this stack, made by
  `docker/thirdparties/docker-compose/iceberg/tools/save_docker.sh` (`mc cp -r minio/warehouse /mnt/data/input/minio`,
  then `zip -rq iceberg_data.zip iceberg_data`). The archive does not contain a license file of its own;
  it is published by the Apache Doris project as a part of its test suite. The tables were created with the statements
  in `docker/thirdparties/docker-compose/iceberg/scripts/create_preinstalled_scripts/iceberg/*.sql` and in the comments
  of the regression test suites (Spark SQL, Flink SQL and the Iceberg Java API). The directory `warehouse` here is
  `iceberg_data/input/minio/warehouse` of the archive, with the same layout: the table locations are
  `s3a://warehouse/...` or `s3://warehouse/...`.

The expected results in the tests are taken from the Doris regression test results
(`regression-test/data/external_table_p0/iceberg/*.out`) of the suites that read these tables:
`iceberg_complex_type`, `iceberg_schema_change`, `iceberg_partition_upper_case_nereids`,
`test_external_catalog_iceberg_partition`, `test_external_catalog_iceberg_common`, `test_iceberg_statistics`,
`test_iceberg_optimize_count`, `test_iceberg_equality_delete`, `test_iceberg_position_delete`,
`test_iceberg_read_with_posdelete`, `test_iceberg_upper_case_column_name`, `test_iceberg_filter`,
`test_iceberg_predicate_conversion`, `test_gen_iceberg_by_api`, `iceberg_read_unitycatalog_table`.
The queries are translated to ClickHouse SQL, and the results are converted to the ClickHouse text format
(for example, `timestamptz` values are shown in `Asia/Shanghai` as in Doris, NULL arrays and maps are read as empty ones).

Changes compared to the originals:

- Only the latest `*.metadata.json` file of each table is kept. The older metadata versions are not used for reading
  the current snapshot, and some tables have more than 30 of them (about 90 KB each).
  All other files (manifest lists and manifests of the older snapshots, data and delete files) are kept as they are.

Tables of the archive that are not included:

- `wh/test_db/table_with_append_file`: its data file `warehouse/data/table_with_append_file/sample-data.snappy.parquet` is 33 MB.
- `wh/test_db/location_s3a_table`: the same as `location_s3_table` with the `s3a://` scheme, which most other tables use.
- `format_v1/sample_parquet`, `format_v2/sample_cow_orc`, `format_v2/sample_cow_parquet`, `format_v2/sample_mor_orc`:
  the same data as `format_v1/sample_orc` and `format_v2/sample_mor_parquet`; Doris only checks their row count.
- `format_v2/sample_mor_paruqet`, `warehouse/tmp_db/taxis3`, `warehouse/tmp_db2/taxis3`: tables without snapshots.
- `nyc/taxis`: not used by the Doris tests.
- `wh/*paimon*`: Paimon tables.
- `be/test/exec/test_data/iceberg_mixed_position_delete_parquet`: a single position delete file, not a table.
