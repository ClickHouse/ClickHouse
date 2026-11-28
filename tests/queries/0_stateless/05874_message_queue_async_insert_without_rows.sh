#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the RabbitMQ engine is not built in the fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH does not need the broker, and nothing listens on port 1.
# The UUID is generated because the flaky check runs this test concurrently with itself.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
    ATTACH TABLE rmq UUID '$uuid' (c0 UInt64) ENGINE = RabbitMQ
    SETTINGS rabbitmq_host_port = '127.0.0.1:1', rabbitmq_username = 'u', rabbitmq_password = 'p',
        rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow'"

# The native protocol sends the data after the query, so the INSERT pipeline built for it is cancelled without running.
reply=$(echo '{"c0": 1}' | timeout 60 $CLICKHOUSE_CLIENT --async_insert 1 --wait_for_async_insert 1 \
    -q "INSERT INTO rmq FORMAT JSONEachRow" 2>&1)
if [ $? -eq 124 ]; then
    # A stuck insert: the cases below would block the asynchronous insert queue for good,
    # and a synchronous DROP would wait for the stuck insert.
    echo "native insert: stuck"
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 -q "DROP TABLE rmq"
    exit 1
fi
echo "native insert: $(grep -o -m1 'CANNOT_CONNECT_RABBITMQ' <<< "$reply")"

# An asynchronous insert whose batch has no rows is flushed without starting the producer.
echo -n '' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query=INSERT%20INTO%20rmq%20FORMAT%20JSONEachRow" --data-binary @-
echo "empty insert: done"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_max_ms=600000" \
    -d 'INSERT INTO rmq FORMAT JSONEachRow {"c0": "x"}'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.asynchronous_inserts WHERE database = currentDatabase() AND table = 'rmq'"
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE rmq"
echo "flush: $?"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.asynchronous_inserts WHERE database = currentDatabase() AND table = 'rmq'"

$CLICKHOUSE_CLIENT --send_logs_level fatal -q "DROP TABLE rmq"
