#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

# The `warnings` and `infos` arrays of /api/v1/query and /api/v1/query_range. The expected strings are
# what Prometheus v3.5.0 returns for the same data, except that it returns them in a random order.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
SET allow_experimental_time_series_table = 1;
DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples)
SELECT name, if(le = '', map('job', job, 'id', id), map('job', job, 'id', id, 'le', le)),
       arrayMap(i -> (toDateTime64(1700000000 + 15 * i, 3), toFloat64(mul * i)), range(41))
FROM values('name String, job String, id String, le String, mul UInt8',
    ('foo', 'a', '0', '', 1), ('foo', 'b', '1', '', 2), ('bar_total', 'a', '2', '', 3), ('qux_count', 'a', '4', '', 1),
    ('total_requests', 'a', '5', '', 1),
    ('m01', 'm', '6', '', 1), ('m02', 'm', '7', '', 1), ('m03', 'm', '8', '', 1), ('m04', 'm', '9', '', 1),
    ('m05', 'm', '10', '', 1), ('m06', 'm', '11', '', 1), ('m07', 'm', '12', '', 1), ('m08', 'm', '13', '', 1),
    ('m09', 'm', '14', '', 1), ('m10', 'm', '15', '', 1), ('m11', 'm', '16', '', 1), ('m12', 'm', '17', '', 1),
    ('http.requests', 'u', '18', '', 1),
    ('req_seconds_bucket', 'a', '19', '0.1', 1), ('req_seconds_bucket', 'a', '19', '1', 2), ('req_seconds_bucket', 'a', '19', '+Inf', 3));

INSERT INTO ts (metric_name, tags, samples) VALUES ('baz', map('job', 'a', 'id', '3'), [(toDateTime64(1700000000, 3), 5)]);
"

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/prometheus/api/v1"

# Prints the status, the number of result series and the response text following the `data` object.
print_response()
{
    python3 -c '
import json, sys
raw = sys.stdin.read().rstrip()
d = json.loads(raw)
starts = [raw.find(key) for key in (",\"warnings\":", ",\"infos\":") if key in raw]
tail = raw[min(starts):-1] if starts else ""
print(d["status"], len(d["data"]["result"]) if d["status"] == "success" else "-", tail)'
}

# instant <time> <query>
instant()
{
    echo "query: $2"
    ${CLICKHOUSE_CURL} -sS --get "${BASE}/query" --data-urlencode "query=$2" --data-urlencode "time=$1" \
        --data-urlencode "database=${CLICKHOUSE_DATABASE}" --data-urlencode "table=ts" \
        --data-urlencode "enable_time_series_aggregate_functions=1" | print_response
}

# range <query>
range()
{
    echo "query_range: $1"
    ${CLICKHOUSE_CURL} -sS --get "${BASE}/query_range" --data-urlencode "query=$1" \
        --data-urlencode "start=1700000000" --data-urlencode "end=1700000600" --data-urlencode "step=60" \
        --data-urlencode "database=${CLICKHOUSE_DATABASE}" --data-urlencode "table=ts" \
        --data-urlencode "enable_time_series_aggregate_functions=1" | print_response
}

T=1700000600

echo "--- quantile: a constant phi outside [0, 1], even without data"
instant $T 'quantile(2, foo)'
instant $T 'quantile((2), foo)'
instant $T 'quantile(-(2), foo)'
instant $T 'quantile(NaN, foo)'
instant $T 'quantile(Inf, foo)'
instant $T 'quantile(1e6, foo)'
instant $T 'quantile(1234567, foo)'
instant $T 'quantile(-0.00001, foo)'
instant $T 'quantile(1.5e-7 - 1, foo)'
instant $T 'quantile(2, nonexistent)'
instant $T 'quantile(0.5, foo)'
instant $T 'quantile(2, foo) + quantile(2, foo)'

echo "--- histogram_quantile: even without data"
instant $T 'histogram_quantile(2, req_seconds_bucket)'
instant $T 'histogram_quantile(2, nonexistent)'

echo "--- quantile_over_time: only if a window has a sample"
instant $T 'quantile_over_time(2, foo[5m])'
instant $T 'quantile_over_time(2, baz[5m])'
instant 1700000100 'quantile_over_time(2, baz[5m])'
instant $T 'quantile_over_time(2, foo[5m:1m])'
instant $T 'sum(quantile_over_time(2, foo[5m])) - avg(quantile_over_time(2, foo[5m]))'

echo "--- rate and increase: each metric name without a counter suffix, only if there is a result"
instant $T 'rate(foo[5m])'
instant $T 'increase(foo[5m])'
instant $T 'irate(foo[5m])'
instant $T 'rate(bar_total[5m])'
instant $T 'rate(total_requests[5m])'
instant $T 'rate(req_seconds_bucket[5m])'
instant $T 'rate(baz[5m])'
instant $T 'rate(nonexistent[5m])'
instant $T 'rate({job="a"}[5m])'
instant 1700000100 'rate({job="a"}[5m])'
instant $T 'rate({__name__=~".+_total"}[5m])'
instant $T 'rate({"http.requests"}[5m])'
instant $T 'rate(foo[5m] offset 1m)'
instant $T 'rate(foo[5m] @ 1700000600)'
instant $T 'rate(foo[5m]) > 1e9'
instant $T 'rate(foo[5m]) + rate(foo[10m])'
instant $T 'sum(rate(foo[5m])) / count(rate(foo[5m]))'
instant $T 'label_replace(rate(foo[5m]), "x", "y", "", "")'
instant $T 'sum(
  rate(
    foo[5m]))'

echo "--- a subquery argument is reported at 1:1, as Prometheus does"
instant $T 'rate(foo[5m:1m])'
instant $T 'rate((foo + 1)[5m:1m])'
instant $T 'rate(bar_total[5m:1m])'
instant $T 'rate({__name__=~"foo|bar_total"}[5m:1m])'
instant $T 'rate((foo offset 1m)[5m:1m])'

echo "--- at most 10 of each kind"
instant $T 'rate({__name__=~"m.+"}[5m])'
instant $T 'quantile(2, rate({__name__=~"m.+"}[5m])) + quantile(3, foo)'

echo "--- range queries"
range 'quantile(2, foo)'
range 'quantile_over_time(2, baz[5m])'
range 'rate(foo[5m])'

echo "--- no annotations in an error"
instant $T 'quantile(2, foo) + rate(foo)'

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
