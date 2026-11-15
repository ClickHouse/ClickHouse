#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# `system.iceberg_labels` shows the labels which an Iceberg REST catalog returns with a table load
# (`labels` of `LoadTableResult`). Labels come with nothing but a table load, so the table costs one request
# to the catalog per table, and a filter on `database` and `table` must limit the requests to the tables the
# query asks for. The stub catalog records the tables which were loaded.

# The failing table load is logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FILE"

python3 - "$PORT_FILE" <<'EOF' &
import json, os, sys, threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, unquote

def field(id, name, type='string'):
    return {'id': id, 'name': name, 'required': False, 'type': type}

def table(fields, labels=None, previous_fields=None):
    schemas = [{'type': 'struct', 'schema-id': 1, 'fields': fields}]
    if previous_fields:
        schemas.insert(0, {'type': 'struct', 'schema-id': 0, 'fields': previous_fields})
    result = {
        'metadata-location': 's3://bucket/table/metadata/v1.metadata.json',
        'metadata': {
            'format-version': 2,
            'table-uuid': '00000000-0000-0000-0000-000000000000',
            'location': 's3://bucket/table',
            'current-schema-id': 1,
            'schemas': schemas,
        },
    }
    if labels is not None:
        result['labels'] = labels
    return result

ADDRESS = {'type': 'struct', 'fields': [field(5, 'city'), field(6, 'zip')]}

# None instead of a table: the catalog refuses to load it.
catalogs = {
    'good': {
        'sales': {
            # Labels of the table and of its fields.
            'orders': table(
                [field(1, 'id', 'long'), field(2, 'customer_email')],
                {
                    'object-labels': {'owner': 'team-checkout', 'classification': 'internal'},
                    'fields': [{'field-id': 2, 'labels': {'pii.type': 'email', 'pii': 'true'}}],
                }),
            # The field 2 was renamed, the field 3 was dropped, the field 5 is nested.
            'customers': table(
                [field(1, 'id', 'long'), field(2, 'full_name'), field(4, 'address', ADDRESS)],
                {
                    'object-labels': {'owner': 'team-sales'},
                    'fields': [
                        {'field-id': 5, 'labels': {'pii.type': 'location'}},
                        {'field-id': 3, 'labels': {'deprecated': 'true'}},
                        {'field-id': 2, 'labels': {'pii': 'true'}},
                    ],
                },
                previous_fields=[field(1, 'id', 'long'), field(2, 'name'), field(3, 'legacy_code')]),
        },
        'hr': {
            # Only labels of the fields.
            'employees': table(
                [field(1, 'id', 'long'), field(2, 'salary', 'double')],
                {'fields': [{'field-id': 2, 'labels': {'classification': 'restricted'}}]}),
            # A catalog which has no labels for a table, in the three ways it can say so.
            'no_labels': table([field(1, 'id', 'long')]),
            'empty_labels': table([field(1, 'id', 'long')], {}),
            'empty_members': table([field(1, 'id', 'long')], {'object-labels': {}, 'fields': []}),
        },
    },
    'bad': {
        'ns': {
            'ok': table([field(1, 'id', 'long')], {'object-labels': {'owner': 'team-a'}}),
            'forbidden': None,
        },
    },
}

loaded = {}
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path, _, query = self.path.partition('?')
        parts = [unquote(part) for part in path.strip('/').split('/')]
        params = parse_qs(query)
        body, code = {}, 404
        if parts == ['ready']:
            code = 200
        elif len(parts) == 2 and parts[0] in catalogs and parts[1] == 'loaded':
            with lock:
                body, code = ' '.join(sorted(loaded.pop(parts[0], ['nothing']))), 200
        elif len(parts) >= 3 and parts[0] in catalogs and parts[1] == 'v1':
            namespaces = catalogs[parts[0]]
            rest = parts[2:]
            if rest == ['config']:
                body, code = {'defaults': {}, 'overrides': {}}, 200
            elif rest == ['namespaces']:
                # The namespaces are not nested.
                names = [] if params.get('parent', [''])[0] else sorted(namespaces)
                body, code = {'namespaces': [[name] for name in names]}, 200
            elif len(rest) == 3 and rest[0] == 'namespaces' and rest[2] == 'tables' and rest[1] in namespaces:
                body = {'identifiers': [{'namespace': [rest[1]], 'name': name} for name in sorted(namespaces[rest[1]])]}
                code = 200
            elif len(rest) == 4 and rest[0] == 'namespaces' and rest[2] == 'tables' and rest[3] in namespaces.get(rest[1], {}):
                with lock:
                    loaded.setdefault(parts[0], []).append(rest[1] + '.' + rest[3])
                body, code = namespaces[rest[1]][rest[3]], 200
                if body is None:
                    body, code = {'error': {'message': 'Forbidden', 'type': 'ForbiddenException', 'code': 403}}, 403
        body = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open(sys.argv[1] + '.tmp', 'w') as f:
    f.write(str(server.server_address[1]))
os.replace(sys.argv[1] + '.tmp', sys.argv[1])
server.serve_forever()
EOF
HTTP_PID=$!

GOOD="${CLICKHOUSE_DATABASE}_good"
BAD="${CLICKHOUSE_DATABASE}_bad"
TEST_USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup() {
    kill $HTTP_PID 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${GOOD}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${BAD}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}" 2>/dev/null
    rm -f "$PORT_FILE"
}
trap cleanup EXIT

for _ in {1..100}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
for _ in {1..100}; do
    if [ "$(${CLICKHOUSE_CURL} -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${HTTP_PORT}/ready")" = "200" ]; then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --allow_database_iceberg=1 --query "
    CREATE DATABASE ${GOOD} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/good/v1')
    SETTINGS catalog_type = 'rest', warehouse = 'wh'"
${CLICKHOUSE_CLIENT} --allow_database_iceberg=1 --query "
    CREATE DATABASE ${BAD} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/bad/v1')
    SETTINGS catalog_type = 'rest', warehouse = 'wh'"

# The tables which the catalog was asked to load since the previous call.
loaded() {
    ${CLICKHOUSE_CURL} -s "http://127.0.0.1:${HTTP_PORT}/$1/loaded"
}

LABELS="SELECT table, scope, field_id, field_name, key, value FROM system.iceberg_labels"
ORDER="ORDER BY table, scope, field_id, key"

echo "-- structure"
${CLICKHOUSE_CLIENT} --query "SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'iceberg_labels' ORDER BY position"

echo "-- all the labels of a catalog"
${CLICKHOUSE_CLIENT} --query "${LABELS} WHERE database = '${GOOD}' ${ORDER}"
echo "loaded: $(loaded good)"

echo "-- one table"
${CLICKHOUSE_CLIENT} --query "${LABELS} WHERE database = '${GOOD}' AND table = 'sales.orders' ${ORDER}"
echo "loaded: $(loaded good)"

echo "-- several tables"
${CLICKHOUSE_CLIENT} --query "${LABELS} WHERE database = '${GOOD}' AND table IN ('sales.orders', 'hr.employees', 'hr.missing') ${ORDER}"
echo "loaded: $(loaded good)"

echo "-- tables of a namespace"
${CLICKHOUSE_CLIENT} --query "${LABELS} WHERE database = '${GOOD}' AND table LIKE 'hr.%' ${ORDER}"
echo "loaded: $(loaded good)"

echo "-- database and table named together"
${CLICKHOUSE_CLIENT} --query "${LABELS} WHERE (database, table) IN (('${GOOD}', 'hr.employees')) ${ORDER}"
echo "loaded: $(loaded good)"

echo "-- a filter on the labels cannot limit the requests"
${CLICKHOUSE_CLIENT} --query "SELECT table, field_name FROM system.iceberg_labels WHERE database = '${GOOD}' AND scope = 'field' AND key = 'pii' AND value = 'true' ORDER BY table, field_id"
echo "loaded: $(loaded good)"

echo "-- a table without labels"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.iceberg_labels WHERE database = '${GOOD}' AND table IN ('hr.no_labels', 'hr.empty_labels', 'hr.empty_members')"
echo "loaded: $(loaded good)"

echo "-- another database"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.iceberg_labels WHERE database = '${GOOD}_missing'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.iceberg_labels WHERE database = 'system'"
echo "loaded: $(loaded good)"

echo "-- a table which the catalog refuses to load"
echo "the query fails: $(${CLICKHOUSE_CLIENT} --query "${LABELS} WHERE database = '${BAD}' ${ORDER}" 2>&1 | grep -o -m1 'RECEIVED_ERROR_FROM_REMOTE_IO_SERVER')"
echo "loaded: $(loaded bad)"
echo "the table is skipped with database_datalake_require_metadata_access = 0:"
${CLICKHOUSE_CLIENT} --database_datalake_require_metadata_access=0 --query "${LABELS} WHERE database = '${BAD}' ${ORDER}"
echo "loaded: $(loaded bad)"
echo "the table is not asked for:"
${CLICKHOUSE_CLIENT} --query "${LABELS} WHERE database = '${BAD}' AND table = 'ns.ok' ${ORDER}"
echo "loaded: $(loaded bad)"

echo "-- access rights"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.iceberg_labels TO ${TEST_USER}"
echo "no grants: $(${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT count() FROM system.iceberg_labels WHERE database = '${GOOD}'")"
echo "loaded: $(loaded good)"

# The labels of the fields tell which columns a table has, so they need SHOW COLUMNS:
# `sales.orders` shows only the labels of the table, `sales.customers` shows the labels of its fields too.
# The tables which the user cannot see are not asked from the catalog.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON ${GOOD}.\`sales.orders\` TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${GOOD}.\`sales.customers\` TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "${LABELS} WHERE database = '${GOOD}' ${ORDER}"
echo "loaded: $(loaded good)"
