#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `sasl.oauthbearer.metadata.authentication.type` set to a value other than `none` (e.g. `azure_imds`),
# librdkafka sends a token of the cloud identity of the server to the brokers. All such values are rejected unless
# the server configuration has `<kafka><allow_oauthbearer_metadata_authentication>`, which the test configuration
# does not have.
# No broker is needed: the check is done when the Kafka client configuration is built.

collection="${CLICKHOUSE_DATABASE}_kafka"
error='Kafka property sasl.oauthbearer.metadata.authentication.type=[a-z_]* is not allowed'

$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${collection}_azure AS
        kafka_broker_list = '127.0.0.1:1',
        kafka_topic_list = 'topic',
        kafka_group_name = 'group',
        kafka_format = 'JSONEachRow',
        \`kafka.sasl_oauthbearer_metadata_authentication_type\` = 'azure_imds';
    CREATE TABLE kafka_azure (x UInt64) ENGINE = Kafka(${collection}_azure);

    CREATE NAMED COLLECTION ${collection}_aws AS
        kafka_broker_list = '127.0.0.1:1',
        kafka_topic_list = 'topic',
        kafka_group_name = 'group',
        kafka_format = 'JSONEachRow',
        \`kafka.sasl_oauthbearer_metadata_authentication_type\` = 'aws_iam';
    CREATE TABLE kafka_aws (x UInt64) ENGINE = Kafka(${collection}_aws);

    -- librdkafka ignores the case of the value, and the flag has no effect in a named collection.
    CREATE NAMED COLLECTION ${collection}_flag AS
        kafka_broker_list = '127.0.0.1:1',
        kafka_topic_list = 'topic',
        kafka_group_name = 'group',
        kafka_format = 'JSONEachRow',
        \`kafka.sasl_oauthbearer_metadata_authentication_type\` = 'AZURE_IMDS',
        \`kafka.allow_oauthbearer_metadata_authentication\` = 'true';
    CREATE TABLE kafka_flag (x UInt64) ENGINE = Kafka(${collection}_flag);
"

echo "azure_imds, consumer:"
$CLICKHOUSE_CLIENT -q "SELECT * FROM kafka_azure SETTINGS stream_like_engine_allow_direct_select = 1" 2>&1 \
    | grep -o "$error" | head -1

echo "azure_imds, producer:"
$CLICKHOUSE_CLIENT -q "INSERT INTO kafka_azure VALUES (1)" 2>&1 \
    | grep -o "$error" | head -1

echo "aws_iam, consumer:"
$CLICKHOUSE_CLIENT -q "SELECT * FROM kafka_aws SETTINGS stream_like_engine_allow_direct_select = 1" 2>&1 \
    | grep -o "$error" | head -1

echo "aws_iam, producer:"
$CLICKHOUSE_CLIENT -q "INSERT INTO kafka_aws VALUES (1)" 2>&1 \
    | grep -o "$error" | head -1

echo "upper case value and the flag in the named collection:"
$CLICKHOUSE_CLIENT -q "SELECT * FROM kafka_flag SETTINGS stream_like_engine_allow_direct_select = 1" 2>&1 \
    | grep -o "$error" | head -1

$CLICKHOUSE_CLIENT -q "
    DROP TABLE kafka_azure;
    DROP TABLE kafka_aws;
    DROP TABLE kafka_flag;
    DROP NAMED COLLECTION ${collection}_azure;
    DROP NAMED COLLECTION ${collection}_aws;
    DROP NAMED COLLECTION ${collection}_flag;
"
