#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a Replicated database does not allow ON CLUSTER

# An entry format before NORMALIZE_CREATE_ON_INITIATOR_VERSION holds the query as written, and the worker
# builds AS src with no user. An initiator that does not hold the source cannot check what the new table
# inherits, so it requires SELECT on the source and TABLE ENGINE on every engine.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"
# No server has this database, so the initiator cannot read the source.
src="${db}_absent.src"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT CLUSTER ON *.* TO ${user};
    GRANT TABLE ENGINE ON MergeTree TO ${user};
"

# Prints the missing privileges, or the error of the worker once the initiator lets the query through.
function try_copy()
{
    local name=$1
    shift
    echo "-- ${name}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version 2 \
        -q "CREATE TABLE ${db}.${name} ON CLUSTER test_shard_localhost AS ${src} $*" 2>&1 \
        | grep -oE "necessary to have the grants SELECT ON [^ ]+ and TABLE ENGINE ON \*|UNKNOWN_DATABASE" | head -n 1 | sed "s/${db}/db/"
}

try_copy copy_without_grants
# A query with its own engine inherits no engine, so it needs no grant on the source.
try_copy copy_own_engine "ENGINE = MergeTree ORDER BY tuple()"

${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${src} TO ${user}"
try_copy copy_with_select

${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON * TO ${user}"
try_copy copy_with_select_and_every_engine

${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON ${src} FROM ${user}"
try_copy copy_with_every_engine

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
