-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: requires `IcebergLocal` (USE_AVRO).
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`
-- https://github.com/ClickHouse/ClickHouse/issues/129912

-- The width applies to the unscaled value.
SELECT icebergTruncate(10, toDecimal32('1.234567', 6));
SELECT icebergTruncate(10, toDecimal32('-1.234567', 6));
SELECT icebergTruncate(10, toDecimal32('-0.000001', 6));
SELECT icebergTruncate(10, toDecimal32('0.123456789', 9));
SELECT icebergTruncate(7, toDecimal32('-0.123456789', 9));
SELECT icebergTruncate(10, toDecimal64('12.3456789012', 10));
SELECT icebergTruncate(10, toDecimal64('-12.3456789012', 10));
SELECT icebergTruncate(100, toDecimal64('-0.123456789012345678', 18));
SELECT icebergTruncate(4, toDecimal128('-0.4896176448678044706833281959', 28));
SELECT icebergTruncate(4, toDecimal128('0.1234567890123456789012345678', 28));
SELECT icebergTruncate(10, toDecimal256('1.2345678901234567890123456789012345678901', 40));
SELECT icebergTruncate(3, toDecimal256('-1.2345678901234567890123456789012345678901', 40));
SELECT icebergTruncate(4294967306, toDecimal32('15', 0));

SELECT toTypeName(icebergTruncate(10, CAST(1, 'Decimal(5, 2)'))), toTypeName(icebergTruncate(10, toDecimal32(1, 4))), toTypeName(icebergTruncate(10, toDecimal64(1, 9)));
SELECT toTypeName(icebergTruncate(10, toDecimal32(1, 6))), toTypeName(icebergTruncate(10, toDecimal64(1, 10))), toTypeName(icebergTruncate(4, toDecimal128(0, 28))), toTypeName(icebergTruncate(3, toDecimal256(1, 40)));
SELECT toTypeName(icebergTruncate(10, toNullable(toDecimal32('1.234567', 6))));

SELECT d, icebergTruncate(10, d) FROM (SELECT arrayJoin(['1.234567', '-1.234567', '0', '5.000004', NULL]::Array(Nullable(Decimal(9, 6)))) AS d);
SELECT d, icebergTruncate(4, d) FROM (SELECT arrayJoin([toDecimal128('-0.4896176448678044706833281959', 28), toDecimal128('0.1234567890123456789012345678', 28), toDecimal128(0, 28)]) AS d);

SELECT icebergTruncate(10, toDecimal32(-2147483648, 0)); -- { serverError DECIMAL_OVERFLOW }

DROP TABLE IF EXISTS t_partitioned;
CREATE TABLE t_partitioned (d Decimal(9, 6)) ENGINE = MergeTree PARTITION BY icebergTruncate(10, d) ORDER BY tuple();
INSERT INTO t_partitioned VALUES ('1.234567'), ('-1.234567'), ('5.000004'), ('0.000009');
SELECT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_partitioned' AND active ORDER BY partition;
SELECT count() FROM t_partitioned WHERE d = toDecimal32('1.234567', 6);
SELECT count() FROM t_partitioned WHERE d < toDecimal32('0.5', 6);
DROP TABLE t_partitioned;

SET allow_insert_into_iceberg = 1;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05565_iceberg_truncate_decimal/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t_iceberg (id Int32, d Decimal(9, 6), w Decimal(28, 28)) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet') PARTITION BY (icebergTruncate(10, d), icebergTruncate(4, w));
INSERT INTO t_iceberg VALUES (1, '1.234567', '0.1234567890123456789012345678'), (2, '-1.234567', '-0.4896176448678044706833281959'), (3, '5.000004', '0');
SELECT id, d, w FROM t_iceberg ORDER BY id;
SELECT id FROM t_iceberg WHERE d < 0 ORDER BY id SETTINGS use_iceberg_partition_pruning = 1, log_comment = '05565_prune_d';
SELECT id FROM t_iceberg WHERE d < 0 ORDER BY id SETTINGS use_iceberg_partition_pruning = 0;
SELECT id FROM t_iceberg WHERE w > 0 ORDER BY id SETTINGS use_iceberg_partition_pruning = 1, log_comment = '05565_prune_w';
SELECT id FROM t_iceberg WHERE w > 0 ORDER BY id SETTINGS use_iceberg_partition_pruning = 0;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['IcebergPartitionPrunedFiles'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05565_prune_d', '05565_prune_w')
ORDER BY log_comment SETTINGS enable_parallel_replicas = 0;
DROP TABLE t_iceberg;
