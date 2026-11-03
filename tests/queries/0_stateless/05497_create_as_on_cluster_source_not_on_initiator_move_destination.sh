#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: the `S3Queue` and `AzureQueue` engines are not built into the fast test build.
# Tag no-replicated-database: a Replicated database does not allow ON CLUSTER

# A `S3Queue` or `AzureQueue` source can hold a move destination, which the worker inherits with no user. An initiator
# that does not hold the source cannot see the destination, so it requires WRITE on the source type without a filter.

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
    GRANT SHOW COLUMNS, SELECT ON ${src} TO ${user};
    GRANT TABLE ENGINE ON * TO ${user};
"

# Prints whether the query misses WRITE on S3, or the error of the worker once the initiator lets the query through.
function try_copy()
{
    echo "-- $1:"
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version 2 \
        -q "CREATE TABLE ${db}.dst ON CLUSTER test_shard_localhost AS ${src}" 2>&1 \
        | grep -oE "WRITE ON S3|UNKNOWN_DATABASE" | head -n 1
}

try_copy without_write

${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON S3('http://localhost:11111/.*') TO ${user}"
try_copy with_write_on_filtered_s3

${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON S3, WRITE ON AZURE TO ${user}"
try_copy with_write

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
