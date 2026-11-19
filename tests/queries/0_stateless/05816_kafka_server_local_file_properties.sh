#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `kafka.*` keys of a named collection are forwarded to librdkafka. Some librdkafka properties refer to
# the filesystem of the server: librdkafka reads, creates, loads or runs the file with the privileges of the server.
# `CREATE NAMED COLLECTION` does not require the privileges to access the files of the server, thus these
# properties are accepted only from a named collection that is defined in the server configuration file.
# No broker is needed: `CREATE TABLE` does not connect, and the configuration of a consumer or a producer
# is checked before the connection.

collection="${CLICKHOUSE_TEST_UNIQUE_NAME}"
required_keys="kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow'"
table_settings="SETTINGS kafka_poll_timeout_ms = 100, kafka_flush_interval_ms = 100"
all_collections=()
all_tables=(kafka_table kafka_probe_table kafka_pem_table kafka_config_table kafka_altered_table kafka_altered_null_table)

function cleanup()
{
    local queries=""
    for name in "${all_tables[@]}"
    do
        queries+=" DROP TABLE IF EXISTS ${name};"
    done
    for name in "${all_collections[@]}"
    do
        queries+=" DROP NAMED COLLECTION IF EXISTS ${name};"
    done
    $CLICKHOUSE_CLIENT -q "$queries"
}
trap cleanup EXIT

# Prints `OK` and the result of the queries, or the message of the error about the Kafka property.
function run()
{
    local output
    local message
    if output=$($CLICKHOUSE_CLIENT -q "$1" 2>&1)
    then
        echo "OK${output:+ $output}"
    else
        message=$(grep -o 'Kafka property .*(BAD_ARGUMENTS)' <<< "$output" | sed 's/: While executing [A-Za-z]*\././' | head -1)
        echo "${message:-$output}"
    fi
}

# Creates a named collection with one extended property and then a table that uses it.
function create_table()
{
    local key="$1"
    local value="$2"
    local table="${3:-kafka_table}"
    local name="${collection}_${#all_collections[@]}"
    all_collections+=("$name")

    echo "${key}: $(run "
        CREATE NAMED COLLECTION ${name} AS ${required_keys}, \`${key}\` = '${value}';
        CREATE TABLE ${table} (x UInt64) ENGINE = Kafka(${name}) ${table_settings};")"
}

# Creates a table with an accepted named collection, and then adds a property to the collection.
function alter_collection_of_existing_table()
{
    local key="$1"
    local table="$2"

    create_table 'kafka.ssl_key_pem' 'contents of the file' "$table"
    $CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION ${all_collections[-1]} SET \`${key}\` = '/etc/passwd'"
    echo "${key}:"
    run "SELECT count() FROM ${table} SETTINGS stream_like_engine_allow_direct_select = 1"
    run "INSERT INTO ${table} VALUES (1)"
    # A table that is read back from its metadata is checked in the same way as a new table.
    $CLICKHOUSE_CLIENT -q "DETACH TABLE ${table}"
    run "ATTACH TABLE ${table}"
    # The table is attached again when the collection does not have the property.
    # `ALTER NAMED COLLECTION ... DELETE` is not used: the formatted query cannot be parsed back if the key has a dot.
    $CLICKHOUSE_CLIENT -q "CREATE OR REPLACE NAMED COLLECTION ${all_collections[-1]} AS ${required_keys}"
    run "ATTACH TABLE ${table}"
}

echo '--- All properties are rejected in a collection created by SQL'
for key in \
    ssl_key_location ssl_certificate_location ssl_ca_location ssl_crl_location ssl_keystore_location \
    ssl_engine_location ssl_engine_id ssl_providers https_ca_location \
    sasl_kerberos_keytab sasl_kerberos_kinit_cmd \
    sasl_oauthbearer_assertion_file sasl_oauthbearer_assertion_private_key_file sasl_oauthbearer_assertion_jwt_template_file \
    plugin_library_paths offset_store_path
do
    create_table "kafka.${key}" '/etc/passwd'
done

echo '--- Other spellings and sections'
create_table 'kafka.ssl.key.location' '/etc/passwd'
create_table 'kafka.topic.offset.store.path' '/etc/passwd'
create_table 'kafka.consumer.ssl_key_location' '/etc/passwd'
create_table 'kafka.producer.ssl_ca_location' '/etc/passwd'
create_table 'kafka.kafka_topic.offset_store_path' '/etc/passwd'
create_table 'kafka.kafka_topic.ssl_ca_location' ''

echo '--- A null character in the name of a property'
# librdkafka gets the name as a C string, thus it uses only the part before the null character:
# without the check, the first key sets `ssl.key.location`.
create_table 'kafka.ssl_key_location\0suffix' '/etc/passwd'
create_table 'kafka.consumer.ssl_key_location\0' '/etc/passwd'
create_table 'kafka.client_rack\0suffix' 'rack'

echo '--- The contents of the file are accepted, and also the value that is not a path'
create_table 'kafka.ssl_ca_location' 'probe' kafka_probe_table
create_table 'kafka.ssl_key_pem' 'contents of the file' kafka_pem_table

echo '--- A collection from the server configuration file is accepted'
run "CREATE TABLE kafka_config_table (x UInt64) ENGINE = Kafka(kafka_config_file_paths) ${table_settings}"

echo '--- Consumers are created for the accepted properties'
# Each consumer waits for the assignment of partitions for 15 seconds, thus the tables are read at the same time.
{
    for table in kafka_probe_table kafka_pem_table kafka_config_table
    do
        echo "${table}: $(run "SELECT count() FROM ${table} SETTINGS stream_like_engine_allow_direct_select = 1")" &
    done
    wait
} | sort

echo '--- A property that is added after the table is created'
alter_collection_of_existing_table 'kafka.ssl_key_location' kafka_altered_table
alter_collection_of_existing_table 'kafka.ssl_key_location\0suffix' kafka_altered_null_table
