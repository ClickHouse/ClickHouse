#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs Avro

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `KILL QUERY` and `max_execution_time` stop a schema registry request while the registry sends its answer slowly.
# The registry is a handler of this server that answers GET and POST with an empty line every 0.5 seconds for 20 seconds.
DB="${CLICKHOUSE_DATABASE}"
H="avro_registry_${DB}"
P="/avro_registry_${DB}"
REGISTRY="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}${P}"

cleanup() { $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`"; }
trap cleanup EXIT
cleanup
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$H\` URL PREFIX '${P}/' METHODS (GET, POST) AS
    SELECT '' FROM numbers(40) WHERE NOT sleepEachRow(0.5) SETTINGS max_block_size = 1 FORMAT TSVRaw"

# Prints the first error name in the output and whether the command took less than 10 seconds. Without the fix it takes 20.
check() {
    local label=$1 start output
    shift
    start=$(date +%s%N)
    output=$("$@" 2>&1)
    echo "$label: $(grep -o -m1 -E 'TIMEOUT_EXCEEDED|QUERY_WAS_CANCELLED|NETWORK_ERROR' <<< "$output" || echo none)" \
        $(( ($(date +%s%N) - start) / 1000000 < 10000 ))
}

QUERY="SELECT * FROM format(AvroConfluent, 'x Int64', unhex('000000000100')) SETTINGS format_avro_schema_registry_url = '$REGISTRY',
    format_avro_schema_registry_max_retries = 0, format_avro_schema_registry_receive_timeout = 60"

check "slow registry, max_execution_time" $CLICKHOUSE_CLIENT -q "$QUERY, max_execution_time = 1, timeout_overflow_mode = 'throw'"
check "slow registry, registration" $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${DB}_05876.avro', 'AvroConfluent', 'x Int64') SELECT 1
    SETTINGS output_format_avro_confluent_subject = 's', format_avro_schema_registry_url = '$REGISTRY',
    format_avro_schema_registry_max_retries = 0, format_avro_schema_registry_receive_timeout = 60,
    max_execution_time = 1, timeout_overflow_mode = 'throw', engine_file_truncate_on_insert = 1"

QUERY_ID="${DB}_kill"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "$QUERY" > "${CLICKHOUSE_TMP}/${QUERY_ID}.out" 2>&1 &
for _ in {1..300}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'")" = 1 ] && break
    sleep 0.1
done
start=$(date +%s%N)
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC FORMAT Null"
wait
echo "slow registry, KILL QUERY: $(grep -o -m1 -E 'TIMEOUT_EXCEEDED|QUERY_WAS_CANCELLED|NETWORK_ERROR' "${CLICKHOUSE_TMP}/${QUERY_ID}.out" || echo none)" \
    $(( ($(date +%s%N) - start) / 1000000 < 10000 ))

# The external data of an HTTP request is read before the query starts, so only `max_execution_time` applies.
# Nothing listens on port 1: the request is retried after 10 seconds. A unique URL, because concurrent requests of the
# same schema id from one registry wait for each other.
EXTERNAL="${CLICKHOUSE_URL}&query=SELECT+count()+FROM+ext&ext_format=AvroConfluent&ext_structure=x+Int64&max_execution_time=1&timeout_overflow_mode=throw"
check "HTTP external data, retries" bash -c "printf '\x00\x00\x00\x00\x01\x00' | ${CLICKHOUSE_CURL} -sS -F 'ext=@-' \
    '${EXTERNAL}&format_avro_schema_registry_url=http://127.0.0.1:1/${DB}&format_avro_schema_registry_max_retries=2&format_avro_schema_registry_retry_initial_backoff_ms=10000'"
check "HTTP external data, slow registry" bash -c "printf '\x00\x00\x00\x00\x01\x00' | ${CLICKHOUSE_CURL} -sS -F 'ext=@-' \
    '${EXTERNAL}&format_avro_schema_registry_url=${REGISTRY}&format_avro_schema_registry_max_retries=0&format_avro_schema_registry_receive_timeout=60'"
