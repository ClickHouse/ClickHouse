#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An ALTER through an Alias must not overwrite the columns that a concurrent ALTER of its target added.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_alias ENGINE = Alias('t')"

for i in {1..20}; do echo "ALTER TABLE t_alias ADD COLUMN a$i UInt8;"; done | ${CLICKHOUSE_CLIENT} --enable_shared_storage_snapshot_in_query 1 &
for i in {1..20}; do echo "ALTER TABLE t ADD COLUMN b$i UInt8;"; done | ${CLICKHOUSE_CLIENT} &
wait

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_alias"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"
