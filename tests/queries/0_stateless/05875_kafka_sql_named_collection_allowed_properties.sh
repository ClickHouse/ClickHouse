#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A named collection created by SQL accepts all librdkafka properties except the ones that use a resource
# of the server: the options of TLS, of Kerberos and of OIDC, and also the properties that are deprecated or
# have no effect. The paths of files stay rejected, see also 05816_kafka_server_local_file_properties.
# No broker is needed: `CREATE TABLE` does not connect, and the configuration of a consumer is checked
# before the connection.

collection="${CLICKHOUSE_TEST_UNIQUE_NAME}"
required_keys="kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow'"
table_settings="SETTINGS kafka_poll_timeout_ms = 100, kafka_flush_interval_ms = 100"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS kafka_table;
        DROP TABLE IF EXISTS kafka_consumer_table;
        DROP NAMED COLLECTION IF EXISTS ${collection};
        DROP NAMED COLLECTION IF EXISTS ${collection}_consumer;"
}
trap cleanup EXIT

# Creates a named collection with one extended property and then a table that uses it.
# Prints `OK`, or the message of the error about the Kafka property. Prints also the value if `$3` is not empty.
function create_table()
{
    local key="$1"
    local value="$2"
    local label="${key}${3:+ = ${value}}"
    local output
    local message

    if output=$($CLICKHOUSE_CLIENT -q "
        CREATE NAMED COLLECTION ${collection} AS ${required_keys}, \`kafka.${key}\` = '${value}';
        CREATE TABLE kafka_table (x UInt64) ENGINE = Kafka(${collection}) ${table_settings};" 2>&1)
    then
        echo "${label}: OK"
    else
        message=$(grep -o 'Kafka property .*(BAD_ARGUMENTS)' <<< "$output" | head -1)
        echo "${label}: ${message:-$output}"
    fi

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kafka_table; DROP NAMED COLLECTION IF EXISTS ${collection};"
}

echo '--- TLS'
create_table 'enable_ssl_certificate_verification' 'false'
create_table 'ssl_endpoint_identification_algorithm' 'none'
create_table 'ssl_cipher_suites' 'ECDHE-RSA-AES256-GCM-SHA384'
create_table 'ssl_curves_list' 'P-256'
create_table 'ssl_sigalgs_list' 'ECDSA+SHA256'
create_table 'ssl_keystore_password' 'secret'

echo '--- Kerberos'
create_table 'sasl_kerberos_service_name' 'kafka2'
create_table 'sasl_kerberos_principal' 'user@EXAMPLE.COM'
create_table 'sasl_kerberos_keytab' '/etc/passwd'
create_table 'sasl_kerberos_kinit_cmd' 'true'

echo '--- OIDC'
create_table 'sasl_oauthbearer_method' 'oidc'
create_table 'sasl_oauthbearer_client_id' 'client'
create_table 'sasl_oauthbearer_client_secret' 'secret'
create_table 'sasl_oauthbearer_scope' 'scope'
create_table 'sasl_oauthbearer_extensions' 'a=b'
create_table 'sasl_oauthbearer_grant_type' 'client_credentials'
create_table 'sasl_oauthbearer_token_endpoint_url' 'https://127.0.0.1:1/token'
create_table 'sasl_oauthbearer_assertion_algorithm' 'ES256'
create_table 'sasl_oauthbearer_assertion_claim_iss' 'issuer'
create_table 'sasl_oauthbearer_assertion_private_key_passphrase' 'secret'
create_table 'sasl_oauthbearer_assertion_private_key_file' '/etc/passwd'
create_table 'sasl_oauthbearer_assertion_file' '/etc/passwd'
create_table 'sasl_oauthbearer_assertion_jwt_template_file' '/etc/passwd'
create_table 'sasl_oauthbearer_metadata_authentication_type' 'azure_imds'

echo '--- The URL of the token endpoint'
create_table 'sasl_oauthbearer_token_endpoint_url' 'http://127.0.0.1:1/token' with_value
create_table 'sasl_oauthbearer_token_endpoint_url' 'file:///etc/passwd' with_value
create_table 'sasl_oauthbearer_token_endpoint_url' '/etc/passwd' with_value
create_table 'sasl_oauthbearer_token_endpoint_url' 'https://127.0.0.1:1/token\0suffix' with_value

echo '--- Properties that are deprecated or have no effect'
create_table 'socket_blocking_max_ms' '1000'
create_table 'broker_version_fallback' '0.10.0'
create_table 'api_version_fallback_ms' '0'
create_table 'client_dns_lookup' 'use_all_dns_ips'
create_table 'socket_nagle_disable' 'true'
create_table 'topic_metadata_refresh_sparse' 'true'
create_table 'topic_blacklist' '^internal'
create_table 'transactional_id' 'id'
create_table 'max_poll_records' '100'
create_table 'ssl_engine_id' 'dynamic'
create_table 'ssl_truststore_location' '/etc/passwd'
create_table 'topic.auto.commit.enable' 'false'
create_table 'topic.enable.auto.commit' 'false'
create_table 'consume_callback_max_messages' '10'

echo '--- Properties that use a resource of the server'
create_table 'offset_store_method' 'file'
create_table 'topic.offset.store.method' 'file'
create_table 'internal_termination_signal' '10'
create_table 'log_queue' 'true'

echo '--- The timeout of a transaction'
create_table 'transaction_timeout_ms' '2147483' with_value
create_table 'transaction_timeout_ms' '2147484' with_value

echo '--- A consumer is created with the accepted properties'
$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${collection}_consumer AS ${required_keys},
        \`kafka.enable_ssl_certificate_verification\` = 'false',
        \`kafka.ssl_endpoint_identification_algorithm\` = 'none',
        \`kafka.sasl_kerberos_service_name\` = 'kafka2',
        \`kafka.client_dns_lookup\` = 'use_all_dns_ips',
        \`kafka.socket_blocking_max_ms\` = '2000',
        \`kafka.consumer.topic_metadata_refresh_sparse\` = 'false';
    CREATE TABLE kafka_consumer_table (x UInt64) ENGINE = Kafka(${collection}_consumer) ${table_settings};"
$CLICKHOUSE_CLIENT --send_logs_level=trace -q 'SELECT count() FROM kafka_consumer_table SETTINGS stream_like_engine_allow_direct_select = 1' 2>&1 \
    | grep -o -E 'Consumer set property (enable\.ssl\.certificate\.verification:false|ssl\.endpoint\.identification\.algorithm:none|sasl\.kerberos\.service\.name:kafka2|client\.dns\.lookup:use_all_dns_ips|socket\.blocking\.max\.ms:2000|topic\.metadata\.refresh\.sparse:false)' \
    | sort -u
