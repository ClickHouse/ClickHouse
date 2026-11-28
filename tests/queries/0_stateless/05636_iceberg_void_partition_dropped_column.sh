#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# A partition field of a dropped source column has the Avro type `null` in the manifests: Spark writes it so after
#   CREATE TABLE t (id bigint, c0 int) USING iceberg PARTITIONED BY (truncate(1, c0)) TBLPROPERTIES ('format-version' = '1');
#   ALTER TABLE t DROP PARTITION FIELD truncate(1, c0); -- in v1 the field stays, with the `void` transform
#   ALTER TABLE t DROP COLUMN c0;
#   INSERT INTO t ...
# Reading such a manifest threw "Cannot get value from Nothing". `data_iceberg/void_partition_dropped_column` is
# a table in that state: written by ClickHouse, with the schema, the partition spec and the manifests changed so.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="$CUR_DIR/data_iceberg/void_partition_dropped_column"

$CLICKHOUSE_LOCAL --query "DESCRIBE icebergLocal('$T') SETTINGS describe_compact_output = 1"
$CLICKHOUSE_LOCAL --query "SELECT id FROM icebergLocal('$T') ORDER BY id"
$CLICKHOUSE_LOCAL --query "SELECT id FROM icebergLocal('$T') WHERE id = 2 SETTINGS use_iceberg_partition_pruning = 1"
$CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$T') SETTINGS use_iceberg_partition_pruning = 0"
