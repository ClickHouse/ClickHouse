#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the RabbitMQ engine is not built in the fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH does not need the broker. Nothing listens on port 1, so the RabbitMQ setup
# of the table never finishes and its streaming task must not run on SYSTEM START or SYSTEM REFRESH.
$CLICKHOUSE_LOCAL --query "
    ATTACH TABLE t UUID '00000000-0000-0000-0000-000000000001' (c0 Int32) ENGINE = RabbitMQ
    SETTINGS rabbitmq_host_port = '127.0.0.1:1', rabbitmq_username = 'u', rabbitmq_password = 'p',
        rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow';
    CREATE TABLE dst (c0 Int32) ENGINE = Memory;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT c0 FROM t;
    SYSTEM START t;
    SYSTEM REFRESH t;
    SYSTEM START ALL BACKGROUND;
    SELECT sleep(1) FORMAT Null;
    SELECT 'OK';
"
