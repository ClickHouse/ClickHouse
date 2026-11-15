# Delta Lake tables from the Trino test suite

The tables in this directory are copied from the Trino Delta Lake connector test resources,
`plugin/trino-delta-lake/src/test/resources` at commit
https://github.com/trinodb/trino/tree/825fd305e559835be0264cc01358ac8a6f8505c3/plugin/trino-delta-lake/src/test/resources
(Apache License 2.0, https://github.com/trinodb/trino/blob/master/LICENSE).

The directory layout is kept: the first directory is the writer that produced the table
(`databricksNNN` - Databricks Runtime N.N, `deltalake` - OSS Delta Lake, `duckdbNNN` - DuckDB,
`trinoNNN` - Trino). Many tables contain the original `README.md` with the statements that
generated them. The expected results in the tests are taken from the Trino tests
(`TestDeltaLakeBasic`, `BaseDeltaLakeConnectorSmokeTest`, `TestDeltaLakeAnalyze` and others)
and from these statements.

Changes compared to the originals:

- `special_paths.tar` contains the tables whose file names contain spaces, `%` or `:`
  (partition values that need escaping): `databricks131/timestamp_ntz_partition`,
  `databricks192/timestamp_tz_partition`, `deltalake/partition_values_parsed_all_types`,
  `deltalake/uri`, `trino410/no_stats_partitions`. Tests extract it into a temporary directory.
- `statistics/*`: Trino ships only the transaction logs for these cases. The `stats` strings of
  the `add` actions are kept as they are; the data files are generated with values matching the
  statistics (`numRecords`, min and max), the paths, sizes and partition values of the `add`
  actions are updated accordingly, and a `protocol` action is added. `basic_multi_file` is not
  included, because its statistics are not consistent (one record with different min and max).

Tables of the Trino test suite that are not included, because they cannot be read correctly yet:

- `databricks153/type_widening_partition`: `ParseError: Failed to parse value '1' as 'decimal(12,2)'`
  from delta-kernel-rs for a partition column widened from an integer type to a decimal.
- `databricks153/variant*`, `databricks154/*variant*`: the `Variant` type is not supported.
- `databricks73/old_timestamps`: `INT96 timestamp out of range` for the year 0100.
- `databricks73/read_timestamps`: the partition value `10000-01-01 00:59:59.999999` cannot be parsed.
- `trino481/timestamp_tz_millis_checkpoint`, `trino481/timestamp_tz_millis_partitioned`: millisecond
  timestamps in the checkpoint are rejected by delta-kernel-rs (`Invalid data type for Delta Lake: Timestamp(ms)`).
- `deltalake/unsupported_writer_version`: rejected by delta-kernel-rs with `InvalidProtocolError`
  (writer features with `minWriterVersion` other than 7), even for reading.
- `databricks73/person*`: every line of the JSON commits has a trailing `x`, which Trino tolerates.
