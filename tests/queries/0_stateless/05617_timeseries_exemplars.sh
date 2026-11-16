#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, zookeeper, no-replicated-database
# no-fasttest: needs protobufs and ANTLR4. no-parallel: reads a counter of system.events. no-replicated-database: ON CLUSTER.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --session_timezone=UTC"

$CLIENT -n -q "
DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS ts_clause;
DROP TABLE IF EXISTS ts_repeated;
DROP TABLE IF EXISTS ts_normalized;
DROP TABLE IF EXISTS ts_ns;
DROP TABLE IF EXISTS ts_policy;
DROP TABLE IF EXISTS ts_copy;
DROP TABLE IF EXISTS ts_tag_column;
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

echo "-- a tag column can't take the name of the exemplars column"
$CLIENT -q "CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS store_exemplars = 1, tags_to_columns = {'job': 'exemplars'}" 2>&1 | grep -o -m1 "is reserved for the exemplars"
$CLIENT -q "CREATE TABLE ts_tag_column ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'exemplars'}"
$CLIENT -q "SELECT count() FROM ts_tag_column"

echo "-- a copy made ON CLUSTER gets the UUID of its exemplars table from the initiator, also with the old entry format"
$CLIENT --distributed_ddl_output_mode=none --distributed_ddl_entry_format_version=2 -q "CREATE TABLE ts_copy ON CLUSTER test_shard_localhost AS ts ENGINE = TimeSeries"
$CLIENT -q "
    SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name LIKE '.inner_id.exemplars.%'
        AND uuid IN (SELECT toUUIDOrZero(extract(query, 'EXEMPLARS INNER UUID \\'([^\\']+)\\''))
                     FROM system.distributed_ddl_queue WHERE query LIKE '%' || currentDatabase() || '%ts_copy%')"

USER_PREFIX="user_05617_${CLICKHOUSE_DATABASE}"
$CLIENT -n -q "
CREATE TABLE ts_repeated ENGINE = TimeSeries SETTINGS store_exemplars = 1;
CREATE TABLE ts_normalized ENGINE = TimeSeries SETTINGS store_exemplars = 1;
CREATE TABLE ts_policy ENGINE = TimeSeries SETTINGS store_exemplars = 1;
CREATE TABLE ts_ns (samples Array(Tuple(DateTime64(9), Float64))) ENGINE = TimeSeries SETTINGS store_exemplars = 1;
DROP USER IF EXISTS ${USER_PREFIX}_granted, ${USER_PREFIX}_revoked, ${USER_PREFIX}_policy, ${USER_PREFIX}_none;
CREATE USER ${USER_PREFIX}_granted, ${USER_PREFIX}_revoked, ${USER_PREFIX}_policy, ${USER_PREFIX}_none;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_PREFIX}_granted, ${USER_PREFIX}_revoked, ${USER_PREFIX}_policy;
REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.ts FROM ${USER_PREFIX}_revoked;
CREATE ROW POLICY policy_05617 ON ${CLICKHOUSE_DATABASE}.ts_policy USING metric_name = 'other_metric' TO ${USER_PREFIX}_policy;
"

BEFORE=$($CLIENT -q "SELECT value FROM system.events WHERE event = 'PrometheusRemoteWriteOutOfOrderExemplars'")

# Writes remote-write requests with exemplars and prints the answers of /api/v1/query_exemplars, one line per request.
# The expected answers were taken from Prometheus 3.5.0 (--enable-feature=exemplar-storage) fed with the same requests.
python3 - "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/prometheus/api/v1" "${CLICKHOUSE_DATABASE}" "${USER_PREFIX}" <<'PYTHON'
import json, struct, sys, urllib.error, urllib.parse, urllib.request

base, database, user_prefix = sys.argv[1], sys.argv[2], sys.argv[3]

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
    try:
        print('write', table, urllib.request.urlopen(request).status)
    except urllib.error.HTTPError as e:
        print('write', table, e.code)

def query(table, params, user=None):
    url = base + '/query_exemplars?' + urllib.parse.urlencode(dict(params, database=database, table=table))
    try:
        response = urllib.request.urlopen(urllib.request.Request(url, headers={'X-ClickHouse-User': user} if user else {}))
        code, body = response.status, response.read().decode()
    except urllib.error.HTTPError as e:
        code, body = e.code, e.read().decode()
    if code in (400, 404):
        body = json.loads(body)['errorType']
    if code == 403:
        body = 'forbidden'
    print(code, body.strip())

write('ts', first)
write('ts', second)
write('ts_plain', first)
write('ts_policy', first)

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

print('-- entries of the same series in one request share the newest accepted exemplar')
R = [('__name__', 'repeated_metric'), ('job', 'r')]
write('ts_repeated',
    series(R, [(T, 1.0)], [([('trace_id', 'n2')], 2.0, T + 2)])
    + series(R, [(T + 60000, 2.0)], [
        ([('trace_id', 'n1')], 1.0, T + 1),  # out of order: older than the exemplar of the first entry
        ([('trace_id', 'n2')], 2.0, T + 2),  # a duplicate of the exemplar of the first entry
        ([('trace_id', 'n3')], 3.0, T + 3),
    ]))
query('ts_repeated', {'query': 'repeated_metric'})

print('-- entries which differ only in an empty label or a repeated label are the same series')
N = [('__name__', 'normalized_metric'), ('job', 'n')]
write('ts_normalized',
    series(N, [(T, 1.0)], [([('trace_id', 'n2')], 2.0, T + 2)])
    + series(N + [('optional', ''), ('job', 'n')], [(T + 60000, 2.0)], [
        ([('trace_id', 'n1')], 1.0, T + 1),  # out of order: older than the exemplar of the first entry
        ([('trace_id', 'n3')], 3.0, T + 3),
    ]))
query('ts_normalized', {'query': 'normalized_metric'})

print('-- a timestamp which does not fit into the timestamp type of the table is refused')
write('ts_ns', series(N, [], [([('trace_id', 'min')], 1.0, -(1 << 63))]))
write('ts_ns', series(N, [(-(1 << 63), 1.0)], []))
query('ts_ns', {'query': 'normalized_metric'})

print('-- a user needs a grant on the table itself and no row policy on it')
query('ts', {'query': 'test_exemplars{job="b"}'}, user_prefix + '_granted')
query('ts', {'query': 'test_exemplars{job="b"}'}, user_prefix + '_revoked')
query('ts_policy', {'query': 'test_exemplars{job="b"}'}, user_prefix + '_policy')
query('ts', {'query': '1 + 1'}, user_prefix + '_revoked')
print('-- the grant is checked before the table is looked up')
query('ts_missing', {'query': 'test_exemplars'}, user_prefix + '_none')
query('ts_missing', {'query': 'test_exemplars'}, user_prefix + '_granted')
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
DROP TABLE ts_repeated;
DROP TABLE ts_normalized;
DROP TABLE ts_ns;
DROP TABLE ts_policy;
DROP TABLE ts_copy;
DROP TABLE ts_tag_column;
DROP ROW POLICY policy_05617 ON ${CLICKHOUSE_DATABASE}.ts_policy;
DROP USER ${USER_PREFIX}_granted, ${USER_PREFIX}_revoked, ${USER_PREFIX}_policy, ${USER_PREFIX}_none;
"
