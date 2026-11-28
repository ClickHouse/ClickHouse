#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no NATS, RabbitMQ or Azure, whose settings it then does not mask

# The masking rules of `nats_url`, `rabbitmq_address` and `after_processing_move_connection_string` render the value
# as an SQL string literal. They used to put it in quotes without escaping it, so a value with a quote or a backslash
# made `SHOW CREATE TABLE` and the statements logged in `system.query_log` invalid SQL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. `SHOW CREATE TABLE` of a `RabbitMQ` table. `ATTACH` into an `Ordinary` database does not connect to the server.
$CLICKHOUSE_LOCAL -q "
    SET allow_deprecated_database_ordinary = 1;
    CREATE DATABASE o ENGINE = Ordinary;
    ATTACH TABLE o.r (a String) ENGINE = RabbitMQ
        SETTINGS rabbitmq_address = 'amqp://host:5672/x\\'y\\\\z', rabbitmq_exchange_name = 'e', rabbitmq_format = 'TSV';
    SELECT extract(create_table_query, 'rabbitmq_address = [^,]*'), isNotNull(formatQueryOrNull(create_table_query))
    FROM system.tables WHERE database = 'o' AND name = 'r'
    FORMAT TSVRaw;
"

# 2. A statement logged in `system.query_log`: the setting is masked whatever the engine, so a `MergeTree` table that
# rejects it is enough.
for setting in nats_url rabbitmq_address after_processing_move_connection_string; do
    $CLICKHOUSE_CLIENT --query_id="05453_${setting}_$CLICKHOUSE_DATABASE" --log_queries=1 -q "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.t (a String) ENGINE = MergeTree ORDER BY a SETTINGS $setting = 'x\\'y\\\\z'" > /dev/null 2>&1
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT extract(query, 'SETTINGS .*'), isNotNull(formatQueryOrNull(query))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id LIKE '05453\\_%' AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
    ORDER BY query_id
    FORMAT TSVRaw"
