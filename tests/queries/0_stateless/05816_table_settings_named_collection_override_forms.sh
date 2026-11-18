#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: depends on librdkafka (the `Kafka` table engine), which is not built in fast test.
#
# `system.table_settings` reads the overrides of a named collection in the engine arguments as the engine's creator does:
# a value that is an identifier stands for its name, a literal value is taken as text, and a key may be a literal or an
# expression, which the stored definition keeps as the literal of its value. The engine does not connect at `CREATE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_override_forms_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS kafka_identifier_value;
        DROP TABLE IF EXISTS kafka_literal_key;
        DROP TABLE IF EXISTS kafka_expression_key;
        DROP TABLE IF EXISTS kafka_number_value;
        DROP NAMED COLLECTION IF EXISTS ${NC};"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC} AS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g';
    CREATE TABLE kafka_identifier_value (a Int) ENGINE = Kafka(${NC}, kafka_format = CSV);
    CREATE TABLE kafka_literal_key (a Int) ENGINE = Kafka(${NC}, 'kafka_format' = 'TSV');
    CREATE TABLE kafka_expression_key (a Int) ENGINE = Kafka(${NC}, concat('kafka_', 'format') = 'JSONEachRow');
    CREATE TABLE kafka_number_value (a Int) ENGINE = Kafka(${NC}, kafka_format = 'CSV', kafka_client_id = 123);"

echo "--- the format each table uses"
${CLICKHOUSE_CLIENT} --query "
    SELECT table, value, changed FROM system.table_settings
    WHERE database = currentDatabase() AND name = 'kafka_format' ORDER BY table"

echo "--- the stored definition keeps the key of an expression as a literal"
${CLICKHOUSE_CLIENT} --query "
    SELECT extract(create_table_query, 'Kafka\\([^,]*, (.*)\\)') FROM system.tables
    WHERE database = currentDatabase() AND name = 'kafka_expression_key'"

echo "--- a number given to a string setting"
${CLICKHOUSE_CLIENT} --query "
    SELECT value, changed FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'kafka_number_value' AND name = 'kafka_client_id'"
