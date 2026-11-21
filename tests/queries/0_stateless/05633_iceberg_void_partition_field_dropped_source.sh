#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# A v1 table keeps a removed partition field as `void`. Once its source column is dropped, Iceberg Java writes
# that field into the manifest partition record with the Avro type `null`.
# `data_iceberg/void_partition_dropped_source` was written with pyiceberg, then the manifest was rewritten to
# match what Iceberg Java writes after these steps:
#   CREATE TABLE t (id bigint, c0 int) USING iceberg PARTITIONED BY (truncate(1, c0)) TBLPROPERTIES ('format-version' = '1');
#   ALTER TABLE t DROP PARTITION FIELD truncate(1, c0);
#   ALTER TABLE t DROP COLUMN c0;
#   INSERT INTO t VALUES (1), (2), (3);

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="$CUR_DIR/data_iceberg/void_partition_dropped_source"

$CLICKHOUSE_LOCAL --query "SELECT * FROM icebergLocal('$T') ORDER BY id"
$CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$T') WHERE id > 1"
