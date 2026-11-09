#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the Prometheus protocols need protobufs and PromQL needs ANTLR4, both disabled in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --session_timezone=UTC"

$CLIENT -n -q "
DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS ts_clause;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS store_exemplars = 1;
CREATE TABLE ts_plain ENGINE = TimeSeries;
"

echo "-- the exemplars table and the outer column exist only with store_exemplars"
$CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'ts' AND name = 'exemplars'"
$CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'ts_plain' AND name = 'exemplars'"
$CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.exemplars.%'"
$CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner_id.exemplars.%' ORDER BY position"
$CLIENT -q "SELECT engine_full LIKE '%MergeTree%ORDER BY (id, timestamp)%' FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.exemplars.%'"

echo "-- an EXEMPLARS clause enables the table and pins the setting"
$CLIENT -q "CREATE TABLE ts_clause ENGINE = TimeSeries EXEMPLARS INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 1024"
$CLIENT -q "SHOW CREATE TABLE ts_clause" | grep -o "store_exemplars = true" | head -1
$CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.exemplars.%'"

echo "-- the exemplars table requires version 12 and can't be disabled with a clause"
$CLIENT -q "CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 11, store_exemplars = 1" 2>&1 | grep -o -m1 "INVALID_SETTING_VALUE"
$CLIENT -q "CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 11 EXEMPLARS INNER ENGINE = MergeTree ORDER BY (id, timestamp)" 2>&1 | grep -o -m1 "INCORRECT_QUERY"
$CLIENT -q "CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS store_exemplars = 0 EXEMPLARS INNER ENGINE = MergeTree ORDER BY (id, timestamp)" 2>&1 | grep -o -m1 "INCORRECT_QUERY"
$CLIENT -q "ALTER TABLE ts MODIFY SETTING store_exemplars = 0" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

BEFORE=$($CLIENT -q "SELECT value FROM system.events WHERE event = 'PrometheusRemoteWriteOutOfOrderExemplars'")

# Writes remote-write requests with exemplars and prints the answers of /api/v1/query_exemplars, one line per request.
# The expected answers were taken from Prometheus 3.5.0 (--enable-feature=exemplar-storage) fed with the same requests.
python3 - "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/prometheus/api/v1" "${CLICKHOUSE_DATABASE}" <<'PYTHON'
import json, struct, sys, urllib.error, urllib.parse, urllib.request

base, database = sys.argv[1], sys.argv[2]

def varint(n):
    n &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)

def field(num, data):
    return varint((num << 3) | 2) + varint(len(data)) + data

def labels(pairs):
    return b''.join(field(1, field(1, n.encode()) + field(2, v.encode())) for n, v in pairs)

def series(lbls, samples, exemplars):
    out = labels(lbls)
    for ts, v in samples:
        out += field(2, varint(9) + struct.pack('<d', v) + varint(16) + varint(ts))
    for lb, v, ts in exemplars:
        out += field(3, labels(lb) + varint(17) + struct.pack('<d', v) + varint(24) + varint(ts))
    return field(1, out)

def snappy(data):
    # Snappy framing with literals only.
    out = bytearray(varint(len(data)))
    for pos in range(0, len(data), 65536):
        chunk = data[pos:pos + 65536]
        n = len(chunk) - 1
        out += bytes([n << 2]) if n < 60 else (bytes([60 << 2, n]) if n < 256 else bytes([61 << 2]) + struct.pack('<H', n))
        out += chunk
    return bytes(out)

T = 1724112000000
A = [('__name__', 'test_exemplars'), ('job', 'a'), ('instance', '1')]
first = (
    series(A, [(T, 1.0), (T + 60000, 2.0)], [
        ([('trace_id', 't1')], 1.0, T + 1),
        ([('trace_id', 't2'), ('span_id', '')], 2.0, T + 2),  # the empty label is dropped
        ([('trace_id', 't2')], 2.0, T + 2),                   # a duplicate of the newest exemplar
        ([('trace_id', 'old')], 5.0, T + 1),                  # out of order: older
        ([('trace_id', 't5')], 1.5, T + 2),                   # out of order: the same timestamp and a smaller value
        ([('trace_id', 'x' * 130)], 3.0, T + 3),              # the labels are too long
        ([('trace_id', 't7')], 3.0, T + 3),
        ([('trace_id', 'tie_a')], 3.0, T + 3),                # the same timestamp and value: ordered by the labels hash
        ([('trace_id', 'tie_b')], 3.0, T + 3),
        ([('trace_id', 'tie_3')], 3.0, T + 3),                # out of order: a smaller labels hash
        ([('trace_id', 'tie_2')], 3.0, T + 3),
        ([('trace_id', 'future')], 9.0, 4102444800000),       # too far in the future
        ([], 4.0, T + 4),
    ])
    + series([('__name__', 'test_exemplars'), ('job', 'b')], [(T, 1.0), (T + 60000, 2.0)], [([('trace_id', 'b1')], 10.0, T + 5000)])
    + series([('__name__', 'other_metric'), ('job', 'a')], [(T, 1.0), (T + 60000, 2.0)], [([('trace_id', 'c1'), ('span_id', 's1')], 7.25, T + 6000)])
    + series([('__name__', 'no_exemplars')], [(T, 1.0), (T + 60000, 2.0)], [])
)
second = series(A, [(T + 120000, 3.0)], [([], 4.0, T + 4), ([('trace_id', 'r2')], 6.0, T + 7)])

def write(table, body):
    request = urllib.request.Request(
        base + '/write?' + urllib.parse.urlencode({'database': database, 'table': table}), data=snappy(body),
        headers={'Content-Type': 'application/x-protobuf', 'Content-Encoding': 'snappy'})
    print('write', table, urllib.request.urlopen(request).status)

def query(table, params):
    url = base + '/query_exemplars?' + urllib.parse.urlencode(dict(params, database=database, table=table))
    try:
        response = urllib.request.urlopen(url)
        code, body = response.status, response.read().decode()
    except urllib.error.HTTPError as e:
        code, body = e.code, e.read().decode()
    if code == 400:
        body = json.loads(body)['errorType']
    print(code, body.strip())

write('ts', first)
write('ts', second)
write('ts_plain', first)

for params in [
    {'query': 'test_exemplars'},
    {'query': 'test_exemplars{job="a"}', 'start': '1724112000.002', 'end': '1724112000.003'},
    {'query': 'test_exemplars{job="b"} or other_metric'},
    {'query': 'rate(test_exemplars{job="a"}[5m])', 'start': '1724112000.004'},
    {'query': 'no_exemplars'},
    {'query': '1 + 1'},
    {'query': 'test_exemplars', 'start': '1724112010', 'end': '1724112005'},
    {'query': 'test_exemplars{'},
    {'query': 'test_exemplars{job=~"a|b"}', 'end': '1724112000.001'},
]:
    query('ts', params)

print('-- a table without the exemplars table')
query('ts_plain', {'query': 'test_exemplars'})
PYTHON

AFTER=$($CLIENT -q "SELECT value FROM system.events WHERE event = 'PrometheusRemoteWriteOutOfOrderExemplars'")
echo "-- out-of-order exemplars counted: $(( ${AFTER:-0} - ${BEFORE:-0} ))"

echo "-- the inner exemplars table keeps the duplicate written by the second request, the reads return it once"
EXEMPLARS_TABLE=$($CLIENT -q "SELECT '.inner_id.exemplars.' || toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts'")
$CLIENT -q "SELECT timestamp, value, labels FROM \`$EXEMPLARS_TABLE\` ORDER BY timestamp, value, toString(labels)"

echo "-- the outer column"
$CLIENT -q "SELECT metric_name, tags, exemplars FROM ts FINAL WHERE notEmpty(exemplars) ORDER BY metric_name, toString(tags) FORMAT TSV"
$CLIENT -q "SELECT metric_name, length(samples), length(exemplars) FROM ts FINAL ORDER BY metric_name, toString(tags) FORMAT TSV"

$CLIENT -n -q "
DROP TABLE ts;
DROP TABLE ts_plain;
DROP TABLE ts_clause;
"
