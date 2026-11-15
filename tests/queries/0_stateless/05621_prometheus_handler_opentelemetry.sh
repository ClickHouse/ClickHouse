#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate some random trace id so that the previous runs of the test do not interfere.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select replaceAll(toString(generateUUIDv4()), '-', '')")
parent_span_id="5150000000000515"

${CLICKHOUSE_CURL} -sS \
    --header "traceparent: 00-$trace_id-$parent_span_id-01" \
    --header "tracestate: some custom state" \
    "${CLICKHOUSE_URL_PROMETHEUS}" > /dev/null

# The span is written when the handler finishes, which happens after the response has been sent to the client.
# Retry a few times to let the handler finish.
for _ in {1..20}
do
    ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
    count=$(${CLICKHOUSE_CLIENT} -q "
        select count() from system.opentelemetry_span_log
        where finish_date >= yesterday()
            and trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
            and operation_name = 'PrometheusRequestHandler'")
    if [[ "$count" -gt 0 ]]; then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "
    select
        operation_name,
        kind,
        lower(hex(parent_span_id)) = '$parent_span_id' as has_parent_span,
        attribute['http.method'],
        attribute['clickhouse.http_status'],
        attribute['clickhouse.uri']
    from system.opentelemetry_span_log
    where finish_date >= yesterday()
        and trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
        and operation_name = 'PrometheusRequestHandler'
    format TSV"
