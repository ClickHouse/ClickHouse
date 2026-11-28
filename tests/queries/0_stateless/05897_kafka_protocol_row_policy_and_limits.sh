#!/usr/bin/env bash

# The Kafka-compatible interface (`kafka_port`) with the restrictions of the users:
# - the end offset of a partition is the same for the users with different row policies;
# - a user without the right to read a topic does not get its offsets;
# - a topic is read in portions with `optimize_move_to_prewhere = 0` as well;
# - the records of a producer are decoded within the memory limit of the user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_PREFIX="user_${CLICKHOUSE_DATABASE}"
USERS="${USER_PREFIX}_filtered, ${USER_PREFIX}_full, ${USER_PREFIX}_no_prewhere, ${USER_PREFIX}_no_access, ${USER_PREFIX}_limited"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE events (id UInt64, data String) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    CREATE TABLE produced (id UInt64, data String) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO events SELECT number, 'first' FROM numbers(10);

    DROP USER IF EXISTS ${USERS};
    CREATE USER ${USER_PREFIX}_filtered IDENTIFIED WITH plaintext_password BY 'password';
    CREATE USER ${USER_PREFIX}_full IDENTIFIED WITH plaintext_password BY 'password';
    CREATE USER ${USER_PREFIX}_no_prewhere IDENTIFIED WITH plaintext_password BY 'password' SETTINGS optimize_move_to_prewhere = 0;
    CREATE USER ${USER_PREFIX}_no_access IDENTIFIED WITH plaintext_password BY 'password';
    CREATE USER ${USER_PREFIX}_limited IDENTIFIED WITH plaintext_password BY 'password' SETTINGS max_memory_usage = '50M';
    GRANT SELECT ON events TO ${USER_PREFIX}_filtered, ${USER_PREFIX}_full, ${USER_PREFIX}_no_prewhere;
    GRANT INSERT ON produced TO ${USER_PREFIX}_limited;

    CREATE ROW POLICY filtered ON events USING id < 5 TO ${USER_PREFIX}_filtered;
    CREATE ROW POLICY full ON events USING 1 TO ${USER_PREFIX}_full, ${USER_PREFIX}_no_prewhere;
"

PYTHONPATH="$CUR_DIR/helpers" python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_KAFKA}" "${CLICKHOUSE_DATABASE}" "${USER_PREFIX}" <<'EOF'
import sys

from kafka_protocol_client import Client

host, port, database, user_prefix = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
topic = f"{database}.events"


def connect(user):
    return Client(host, port, f"{user_prefix}_{user}", "password")


def block_offset(offset):
    return offset & 0xFFFFFFFF


print("Row policies")
# The user with the row policy reads first. The end offset is not of its last visible row.
with connect("filtered") as client:
    error, high_watermark, batches, next_offset = client.fetch(topic, 0, 0)
    records = [record for batch in batches for record in batch]
    print("filtered:", error, len(records), block_offset(high_watermark), block_offset(client.list_offset(topic, 0, -1)[1]))
    # After the last visible record, an empty batch moves the consumer to the end offset.
    error, high_watermark, batches, next_offset = client.fetch(topic, 0, records[-1][0] + 1)
    print("filtered, after the visible records:", error, batches, next_offset == high_watermark)

with connect("full") as client:
    error, high_watermark, batches, next_offset = client.fetch(topic, 0, 0)
    records = [record for batch in batches for record in batch]
    print("full:", error, len(records), block_offset(high_watermark), next_offset == high_watermark)

print("No access")
TOPIC_AUTHORIZATION_FAILED = 29
with connect("no_access") as client:
    error, high_watermark, batches, next_offset = client.fetch(topic, 0, 0)
    print("fetch:", error == TOPIC_AUTHORIZATION_FAILED, high_watermark, batches)
    print("the end offset:", client.list_offset(topic, 0, -1) == (TOPIC_AUTHORIZATION_FAILED, -1))
EOF

# More blocks than a fetch of this size returns.
for i in {1..5}; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO events SELECT ${i} * 1000 + number, repeat('x', 1000) FROM numbers(100)"
done

PYTHONPATH="$CUR_DIR/helpers" python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_KAFKA}" "${CLICKHOUSE_DATABASE}" "${USER_PREFIX}" <<'EOF'
import sys

from kafka_protocol_client import Client, record_batch

host, port, database, user_prefix = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]


def connect(user):
    return Client(host, port, f"{user_prefix}_{user}", "password")


print("Reading in portions without the automatic PREWHERE")
topic = f"{database}.events"
with connect("no_prewhere") as client:
    offset = 0
    num_records = 0
    num_fetches = 0
    while True:
        error, high_watermark, batches, next_offset = client.fetch(topic, 0, offset, max_bytes=20000)
        assert error == 0, error
        if offset == high_watermark:
            break
        records = [record for batch in batches for record in batch]
        assert all(record[0] >= offset for record in records)
        num_records += len(records)
        num_fetches += 1
        offset = next_offset
    print("records:", num_records, "more than one fetch:", num_fetches > 1)

print("Memory limit of a producer")
MESSAGE_TOO_LARGE = 10
topic = f"{database}.produced"
with connect("limited") as client:
    print("small batch:", client.produce(topic, 0, record_batch([b'{"id": 1, "data": "small"}'], compress=True)))
    # About 128 KiB of gzip is decompressed into 128 MiB, which is more than `max_memory_usage` of the user.
    large = b'{"id": 2, "data": "' + b"x" * (128 << 20) + b'"}'
    print("large batch:", client.produce(topic, 0, record_batch([large], compress=True)) == MESSAGE_TOO_LARGE)
    print("small batch:", client.produce(topic, 0, record_batch([b'{"id": 3, "data": "small"}'], compress=True)))
EOF

$CLICKHOUSE_CLIENT -q "
    SELECT id, data FROM produced ORDER BY id;
    DROP ROW POLICY filtered ON events;
    DROP ROW POLICY full ON events;
    DROP USER ${USERS};
"
