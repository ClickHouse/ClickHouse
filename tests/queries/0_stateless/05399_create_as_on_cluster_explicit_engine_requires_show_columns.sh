#!/usr/bin/env bash

# Tags: no-replicated-database
# Tag no-replicated-database: a Replicated database does not allow ON CLUSTER

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT CLUSTER ON *.* TO ${user};
    GRANT TABLE ENGINE ON MergeTree TO ${user};
    CREATE TABLE ${db}.plain_src (id UInt64) ENGINE = MergeTree ORDER BY id;
"

echo "without SHOW COLUMNS:"
${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw \
    --distributed_ddl_entry_format_version 2 \
    -q "CREATE TABLE ${db}.copy ON CLUSTER test_shard_localhost ENGINE = MergeTree ORDER BY id AS ${db}.plain_src" 2>&1 \
    | grep -oE "necessary to have the grant SHOW COLUMNS ON ${db}\.plain_src" | sed "s/${db}/db/"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
