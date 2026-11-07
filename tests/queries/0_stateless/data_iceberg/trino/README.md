# Iceberg tables from the Trino test suite

The tables in this directory are copied without changes from the Trino Iceberg connector test resources,
`plugin/trino-iceberg/src/test/resources/iceberg` at commit
https://github.com/trinodb/trino/tree/29d63fc2d33560875ead567e6bc8200e5e67bee5/plugin/trino-iceberg/src/test/resources/iceberg
(Apache License 2.0, https://github.com/trinodb/trino/blob/master/LICENSE). Each table contains the
original `README.md` with the statements that generated it.

- `timetravel`: one insert per day, for time travel by timestamp (`TestIcebergReadVersionedTableByTemporal`).
- `conflict_truncate`: two partition specs with a field of the same name, `truncate(a, 1)` and
  `truncate(a, 10)` (`TestIcebergPartitionEvolutionOnSameColumn`).
- `invalid_compression_codec/{none_avro,none_parquet,orc_gzip}`: Avro, Parquet and ORC data files of
  tables with a compression codec property that is invalid for the format (`TestIcebergInvalidCompressionCodecs`).
- `mixed_case_bloom_filter`: an empty table with a mixed-case column name and bloom filter property
  (`TestIcebergParquetWithBloomFiltersMixedCase`).

The expected results in the tests are taken from these Trino tests. The tables keep their original
`s3://...` locations, which ClickHouse replaces with the local table directory.

The Delta Lake UniForm tables `databricks133/uniform_iceberg_v1` and `databricks143/uniform_iceberg_v2`
of `plugin/trino-delta-lake/src/test/resources` are also read as Iceberg tables
(`05768_iceberg_trino_uniform`); they are not copied here, the test uses them from `data_delta_lake/trino`.

Not included:

- `materialized_view_expired_snapshots`: incomplete, the manifests of the expired snapshots are not
  in the repository, and the expected results depend on Trino's materialized view metadata.
- `testing/trino-tests/src/test/resources/iceberg/tpch` and `tpcds`: metadata only, without data files.
