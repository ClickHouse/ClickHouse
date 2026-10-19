#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: the test creates a `Remote` database that points to an unreachable server, and a concurrent
# query that scans `system.tables` over all databases would fail while it exists
# Tag no-replicated-database: a Replicated database does not allow ON CLUSTER

# With an entry format before NORMALIZE_CREATE_ON_INITIATOR_VERSION, CREATE TABLE AS with its own ENGINE inherits
# no definition from the source, so the initiator does not look the source up. For a `Remote` database that lookup
# would connect to the server behind it before any grant is checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
remote_db="${CLICKHOUSE_DATABASE}_remote"

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${remote_db};
    CREATE DATABASE ${remote_db} ENGINE = Remote('127.0.0.1:1', 'private_db', 'remote_user', 'password');
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
"

# A user without grants must get an access error, not the error of a connection to the server behind the source.
for version in 1 2; do
    echo "-- entry format ${version}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version "${version}" \
        -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost AS ${remote_db}.src ENGINE = Memory" 2>&1 \
        | grep -oE '\((ACCESS_DENIED|NO_REMOTE_SHARD_AVAILABLE)\)' | head -n 1
done

${CLICKHOUSE_CLIENT} -q "
    DROP USER ${user};
    DROP DATABASE ${remote_db};
"
