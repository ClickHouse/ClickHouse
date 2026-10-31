#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A text bloom filter index with the total size of filters over the limit cannot be created anymore,
# but a table created before the check still loads and can be altered, as long as its index does not grow.

UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -m -q "
ATTACH TABLE t_bf_total_size_attach UUID '$UUID' (a String, b String, INDEX idx (a, b) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_bf_total_size_attach ADD COLUMN c String;
ALTER TABLE t_bf_total_size_attach RENAME COLUMN b TO d;
ALTER TABLE t_bf_total_size_attach ADD INDEX idx2 (a, c) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_bf_total_size_attach' ORDER BY position;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_bf_total_size_attach';
DROP TABLE t_bf_total_size_attach;
"
