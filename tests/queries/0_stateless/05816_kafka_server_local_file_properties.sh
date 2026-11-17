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

function cleanup()
{
    local queries="DROP TABLE IF EXISTS kafka_table; DROP TABLE IF EXISTS kafka_config_table;"
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
    local name="${collection}_${#all_collections[@]}"
    all_collections+=("$name")

    echo "${key}: $(run "
        CREATE NAMED COLLECTION ${name} AS ${required_keys}, \`${key}\` = '${value}';
        CREATE TABLE kafka_table (x UInt64) ENGINE = Kafka(${name}) ${table_settings};")"
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

echo '--- The contents of the file are accepted, and also the value that is not a path'
create_table 'kafka.ssl_ca_location' 'probe'
$CLICKHOUSE_CLIENT -q "DROP TABLE kafka_table"
create_table 'kafka.ssl_key_pem' 'contents of the file'

echo '--- A collection from the server configuration file is accepted'
run "CREATE TABLE kafka_config_table (x UInt64) ENGINE = Kafka(kafka_config_file_paths) ${table_settings}"

echo '--- A property that is added after the table is created'
$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION ${all_collections[-1]} SET \`kafka.ssl_key_location\` = '/etc/passwd'"
run "SELECT count() FROM kafka_table SETTINGS stream_like_engine_allow_direct_select = 1"
run "INSERT INTO kafka_table VALUES (1)"
# The table is loaded from its metadata, but it cannot create a consumer.
run "DETACH TABLE kafka_table; ATTACH TABLE kafka_table;"
run "SELECT count() FROM kafka_table SETTINGS stream_like_engine_allow_direct_select = 1"

echo '--- Consumers are created for the accepted properties'
$CLICKHOUSE_CLIENT -q "
    ALTER NAMED COLLECTION ${all_collections[-1]} DELETE \`kafka.ssl_key_location\`;
    ALTER NAMED COLLECTION ${all_collections[-1]} SET \`kafka.ssl_ca_location\` = 'probe';"
# Each consumer waits for the assignment of partitions for 15 seconds, thus the tables are read in parallel.
run "
    SELECT count() FROM (SELECT x FROM kafka_table UNION ALL SELECT x FROM kafka_config_table)
    SETTINGS stream_like_engine_allow_direct_select = 1, max_threads = 2"
