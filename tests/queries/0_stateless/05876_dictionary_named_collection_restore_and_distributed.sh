#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: the MySQL integration is not available in the fast test build.

# `RESTORE` creates a dictionary with an internal query, so it requires `NAMED COLLECTION` on the collection of the source
# itself, as `CREATE DICTIONARY` does. A dictionary that is created on other hosts (`ON CLUSTER`, a `Replicated` database,
# `RESTORE ON CLUSTER`) is built there with full access, and the collection there can store other keys, so any key
# that the source sets besides `name` requires `SHOW NAMED COLLECTIONS SECRETS` on the collection.
# The dictionaries are lazy and never loaded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"
nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc"
replicated_db="${CLICKHOUSE_DATABASE}_replicated"

function cleanup()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP DICTIONARY IF EXISTS d;
        DROP DATABASE IF EXISTS ${replicated_db} SYNC;
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS $nc;"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE NAMED COLLECTION $nc AS host = '127.0.0.1', port = 1, user = 'u', password = 'p', db = 'd';
    CREATE DATABASE ${replicated_db} ENGINE = Replicated('/clickhouse/{database}/${replicated_db}', 'shard_1', 'replica_1');
    CREATE USER $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${replicated_db}.* TO $user;
    GRANT CLUSTER ON *.* TO $user;
    GRANT READ ON DISK TO $user;"

# Prints whether the query of the user was denied for the grant (a regular expression), and the number of dictionaries `d`
# in the database, then drops it.
function run_as_user()
{
    local database="$1"
    local grant="$2"
    local query="$3"
    $CLICKHOUSE_CLIENT --user "$user" --distributed_ddl_output_mode=none --query "$query" 2>&1 \
        | grep -qE "necessary to have the grant ${grant}" && echo 1 || echo 0
    $CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --multiquery --query "
        SELECT count() FROM system.tables WHERE database = '${database}' AND name = 'd';
        DROP DICTIONARY IF EXISTS ${database}.d SYNC;"
}

function create_query()
{
    local database="$1"
    local on_cluster="$2"
    local source="$3"
    echo "CREATE DICTIONARY ${database}.d ${on_cluster} (id UInt64, value String) PRIMARY KEY id
        SOURCE(MYSQL(${source})) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1)"
}

# The backups are made once, by `default`: one with the collection only, one with an override.
backup_plain="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_plain')"
backup_override="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_override')"
$CLICKHOUSE_CLIENT --multiquery --query "
    $(create_query "${CLICKHOUSE_DATABASE}" "" "NAME $nc TABLE 't'");
    BACKUP DICTIONARY ${CLICKHOUSE_DATABASE}.d TO ${backup_plain} FORMAT Null;
    DROP DICTIONARY ${CLICKHOUSE_DATABASE}.d;
    $(create_query "${CLICKHOUSE_DATABASE}" "" "NAME $nc HOST 'other.example'");
    BACKUP DICTIONARY ${CLICKHOUSE_DATABASE}.d TO ${backup_override} FORMAT Null;
    DROP DICTIONARY ${CLICKHOUSE_DATABASE}.d;"

NC_GRANT="NAMED COLLECTION ON \`?${nc}\`?\."
# With another missing grant, the message lists both: `SHOW NAMED COLLECTIONS SECRETS, NAMED COLLECTION ON nc`.
SECRETS_GRANT="SHOW NAMED COLLECTIONS SECRETS[A-Z, ]* ON \`?${nc}\`?\."

echo '--- RESTORE requires NAMED COLLECTION'
echo 'Without the grant: denied, not restored'
run_as_user "${CLICKHOUSE_DATABASE}" "$NC_GRANT" "RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d FROM ${backup_plain} FORMAT Null"
echo 'Without the grant, the source replaced by Null: restored'
run_as_user "${CLICKHOUSE_DATABASE}" "$NC_GRANT" "RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d FROM ${backup_plain} SETTINGS restore_replace_external_dictionary_source_to_null = 1 FORMAT Null"
$CLICKHOUSE_CLIENT --query "GRANT NAMED COLLECTION ON $nc TO $user"
echo 'With the grant: restored'
run_as_user "${CLICKHOUSE_DATABASE}" "$NC_GRANT" "RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d FROM ${backup_plain} FORMAT Null"

echo '--- Keys of the collection on other hosts require SHOW NAMED COLLECTIONS SECRETS'
echo 'ON CLUSTER, only the name: created'
run_as_user "${CLICKHOUSE_DATABASE}" "$SECRETS_GRANT" "$(create_query "${CLICKHOUSE_DATABASE}" "ON CLUSTER test_shard_localhost" "NAME $nc")"
echo 'ON CLUSTER, a key that the collection does not store: denied, not created'
run_as_user "${CLICKHOUSE_DATABASE}" "$SECRETS_GRANT" "$(create_query "${CLICKHOUSE_DATABASE}" "ON CLUSTER test_shard_localhost" "NAME $nc TABLE 't'")"
echo 'Replicated database, a stored key: denied, not created'
run_as_user "${replicated_db}" "$SECRETS_GRANT" "$(create_query "${replicated_db}" "" "NAME $nc HOST 'other.example'")"
echo 'Local, a key that the collection does not store: created'
run_as_user "${CLICKHOUSE_DATABASE}" "$SECRETS_GRANT" "$(create_query "${CLICKHOUSE_DATABASE}" "" "NAME $nc TABLE 't'")"
echo 'RESTORE ON CLUSTER with an override: denied, not restored'
run_as_user "${CLICKHOUSE_DATABASE}" "$SECRETS_GRANT" "RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d ON CLUSTER test_shard_localhost FROM ${backup_override} FORMAT Null"
echo 'RESTORE into a Replicated database with an override: denied, not restored'
run_as_user "${replicated_db}" "$SECRETS_GRANT" "RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d AS ${replicated_db}.d FROM ${backup_override} FORMAT Null"
$CLICKHOUSE_CLIENT --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc TO $user"
echo 'With the grant, ON CLUSTER: created'
run_as_user "${CLICKHOUSE_DATABASE}" "$SECRETS_GRANT" "$(create_query "${CLICKHOUSE_DATABASE}" "ON CLUSTER test_shard_localhost" "NAME $nc TABLE 't'")"
echo 'With the grant, Replicated database: created'
run_as_user "${replicated_db}" "$SECRETS_GRANT" "$(create_query "${replicated_db}" "" "NAME $nc HOST 'other.example'")"
echo 'With the grant, RESTORE ON CLUSTER: restored'
run_as_user "${CLICKHOUSE_DATABASE}" "$SECRETS_GRANT" "RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d ON CLUSTER test_shard_localhost FROM ${backup_override} FORMAT Null"
