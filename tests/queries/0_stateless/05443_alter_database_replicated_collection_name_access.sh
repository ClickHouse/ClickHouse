#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: the test creates a Replicated database of its own

# Setting `collection_name` of a `Replicated` database installs the credentials of the collection into
# the database cluster, which then connects with them on behalf of every user of the database. So the
# user who sets it needs the `NAMED COLLECTION` grant on the collection, not only `ALTER DATABASE SETTINGS`.
#
# The credentials are checked by connecting through the database cluster: one server is enough, since
# `prefer_localhost_replica = 0` makes the initiator connect over TCP to its own replica.

# Keep the server-side authentication errors out of the captured client output, so the probe below
# reports the query outcome and nothing else. This has to be set before shell_config.sh.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_replicated"
user="${CLICKHOUSE_DATABASE}_user"
cluster_user="${CLICKHOUSE_DATABASE}_cluster_user"
# Named collections are server-global, so the name is derived from the test database to keep
# concurrent runs of the test apart.
collection="${CLICKHOUSE_DATABASE}_collection"

trap '$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db; DROP USER IF EXISTS $user, $cluster_user; DROP NAMED COLLECTION IF EXISTS $collection"' EXIT

$CLICKHOUSE_CLIENT -q "
CREATE DATABASE $db ENGINE = Replicated('/test/$CLICKHOUSE_DATABASE/collection_name_access', 'shard1', 'replica1');
CREATE USER $cluster_user IDENTIFIED WITH plaintext_password BY 'cluster_password';
CREATE NAMED COLLECTION $collection AS cluster_username = '$cluster_user', cluster_password = 'cluster_password',
    cluster_secure_connection = 'false';
CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER DATABASE SETTINGS ON $db.* TO $user;
"

function alter_collection_name_as_user()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user" --password "password" -q "ALTER DATABASE $db MODIFY SETTING collection_name = '$1'" 2>&1)
    if echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    elif [ -z "$output" ]; then
        echo "OK"
    else
        echo "$output"
    fi
}

# The user the database cluster connects with, and the definition stored in the metadata file.
# Reading `system.clusters` also builds the cluster object the database caches.
function print_state()
{
    $CLICKHOUSE_CLIENT --param_label "$1" --param_db "$db" -q "
    SELECT {label:String},
        (SELECT replaceAll(user, currentDatabase(), '{db}') FROM system.clusters WHERE cluster = {db:String}),
        replaceAll(engine_full, currentDatabase(), '{db}')
    FROM system.databases WHERE name = {db:String}"
}

# Run a query through the database cluster. A failure is reported as a single error-code token, so an
# unrelated failure cannot be mistaken for an authentication one (it produces an empty token instead).
function probe()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --prefer_localhost_replica 0 --connections_with_failover_max_tries 1 \
        -q "SELECT count() FROM clusterAllReplicas('$db', system.one)" 2>&1); then
        echo "$1 ok rows=$output"
    else
        echo "$1 failed $(echo "$output" | grep -oE 'AUTHENTICATION_FAILED|ALL_CONNECTION_TRIES_FAILED|NETWORK_ERROR|SOCKET_TIMEOUT' | head -1)"
    fi
}

print_state 'created'
probe 'created'

# Denied for an existing collection, and for a missing one alike, so the error does not reveal
# whether a collection exists.
alter_collection_name_as_user "$collection"
alter_collection_name_as_user "${collection}_missing"

print_state 'denied'

$CLICKHOUSE_CLIENT -q "GRANT NAMED COLLECTION ON $collection TO $user"

alter_collection_name_as_user "$collection"

print_state 'granted'
probe 'granted'

# The database keeps the credentials it read from the collection, so changing the collection alone does
# not affect it. Setting the same `collection_name` again rereads the collection.
$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION $collection SET cluster_password = 'wrong_password'"

probe 'collection changed'

alter_collection_name_as_user "$collection"

probe 'collection reread'

# The changed `collection_name` survives a reload from the metadata file.
$CLICKHOUSE_CLIENT -q "
ALTER NAMED COLLECTION $collection SET cluster_password = 'cluster_password';
DETACH DATABASE $db;
ATTACH DATABASE $db;
SYSTEM SYNC DATABASE REPLICA $db;
"

print_state 'reattached'
probe 'reattached'

# Clearing `collection_name` uses no collection, so it needs no grant.
$CLICKHOUSE_CLIENT -q "REVOKE NAMED COLLECTION ON $collection FROM $user"

alter_collection_name_as_user ""

print_state 'cleared'
probe 'cleared'
