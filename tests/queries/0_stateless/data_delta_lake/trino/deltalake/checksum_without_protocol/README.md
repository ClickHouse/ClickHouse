Data generated using Apache Spark 4.0.0 & Delta Lake OSS 4.0.0

This test resource is used to verify whether reading Delta metadata and protocol information from
checksum files (rather than from the Delta log) works as expected.

From https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file:

> The Delta transaction log must remain an append-only log. To enable the detection of
> non-compliant modifications to Delta files, writers can optionally emit an auxiliary file with
> every commit, which contains important information about the state of the table as of that
> version.

Spark SQL:
```
CREATE SCHEMA checksum_without_protocol LOCATION 'file:///tmp/checksum_without_protocol';
CREATE TABLE checksum_without_protocol.checksum_without_protocol (data INTEGER) USING DELTA TBLPROPERTIES (delta.checkpointInterval = 1);
INSERT INTO checksum_without_protocol.checksum_without_protocol values 1;
```

Subsequently, the `protocol` element was manually removed from all checksum files in the
`_delta_log`. This fixture is otherwise identical to `deltalake/checksum`.
