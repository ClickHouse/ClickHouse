#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A text bloom filter index with the total size of filters over the limit cannot be created anymore,
# but a table created before the check still loads and can be altered, as long as its index does not grow.

UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# A full-definition ATTACH is validated like CREATE.
$CLICKHOUSE_CLIENT -q "
ATTACH TABLE t_bf_total_size_attach UUID '$UUID' (a String, b String, INDEX idx (a, b) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
"

# The stored definition is rewritten to carry the oversized index and then attached back.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_bf_total_size_attach (a String, b String, INDEX idx (a, b) TYPE tokenbf_v1(1024, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple()"
metadata_file=$($CLICKHOUSE_CLIENT -q "
    SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 't_bf_total_size_attach'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_bf_total_size_attach"
sed -i.bak 's/tokenbf_v1(1024, 3, 0)/tokenbf_v1(536870913, 3, 0)/' "$metadata_file" && rm -f "$metadata_file.bak"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_bf_total_size_attach"

$CLICKHOUSE_CLIENT -m -q "
SELECT type_full FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_bf_total_size_attach';
ALTER TABLE t_bf_total_size_attach ADD COLUMN c String;
ALTER TABLE t_bf_total_size_attach RENAME COLUMN b TO d;
ALTER TABLE t_bf_total_size_attach ADD INDEX idx2 (a, c) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_bf_total_size_attach' ORDER BY position;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_bf_total_size_attach';
DROP TABLE t_bf_total_size_attach;
"
