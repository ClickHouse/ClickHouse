#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the RabbitMQ engine is not built in the fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH does not need the broker. Nothing listens on port 1,
# so an INSERT whose pipeline was built fails in the producer with "Cannot connect to RabbitMQ".
$CLICKHOUSE_LOCAL --ignore-error --query "
    ATTACH TABLE t UUID '00000000-0000-0000-0000-000000000001' (c0 Int32, c1 UInt64 MATERIALIZED 7) ENGINE = RabbitMQ
    SETTINGS rabbitmq_host_port = '127.0.0.1:1', rabbitmq_username = 'u', rabbitmq_password = 'p',
        rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow';
    INSERT INTO t VALUES (1);
    INSERT INTO t (c0, c1) SETTINGS insert_allow_materialized_columns = 1 VALUES (1, 2);
    CREATE TABLE src (c0 Int32) ENGINE = Null;
    CREATE MATERIALIZED VIEW mv TO t AS SELECT c0 FROM src;
    INSERT INTO src VALUES (1);
" 2>&1 | grep -o -E "Cannot connect to RabbitMQ|Block structure mismatch"
