#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no Arrow Flight (ENABLE_LIBRARIES=0)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Arrow Flight SQL catalog commands and GetSessionOptions run SQL that the server builds over system tables,
# so they must work in a session with `use_query_cache = 1`, while a query of the client still honours the setting.
python3 - "${CLICKHOUSE_HOST:-localhost}" "${CLICKHOUSE_PORT_ARROWFLIGHT:-9090}" <<'EOF'
import sys
import uuid

import pyarrow as pa
from pyarrow import flight

client = flight.FlightClient(f"grpc://{sys.argv[1]}:{sys.argv[2]}")
token = client.authenticate_basic_token("default", "")
opts = flight.FlightCallOptions(headers=[token, (b"x-clickhouse-session-id", str(uuid.uuid4()).encode())], timeout=30)


def command(name, value=b""):
    url = f"type.googleapis.com/arrow.flight.protocol.sql.{name}".encode()
    return flight.FlightDescriptor.for_command(b"\x0a" + bytes([len(url)]) + url + b"\x12" + bytes([len(value)]) + value)


writer, _ = client.do_put(
    flight.FlightDescriptor.for_command("SET use_query_cache = 1, query_cache_system_table_handling = 'throw'"), pa.schema([]), opts)
writer.close()

calls = {
    "GetDbSchemas": lambda: client.get_flight_info(command("CommandGetDbSchemas"), opts),
    "GetTables": lambda: client.get_flight_info(command("CommandGetTables"), opts),
    "GetTableTypes": lambda: client.get_flight_info(command("CommandGetTableTypes"), opts),
    "GetPrimaryKeys": lambda: client.get_flight_info(command("CommandGetPrimaryKeys", b"\x1a\x03one"), opts),
    "GetSchema of GetTables": lambda: client.get_schema(command("CommandGetTables"), opts),
    "GetSessionOptions": lambda: list(client.do_action(flight.Action("GetSessionOptions", b""), opts)),
    "SELECT FROM system.one": lambda: client.get_flight_info(flight.FlightDescriptor.for_command("SELECT * FROM system.one"), opts),
}

for name, call in calls.items():
    try:
        call()
        print(name, "OK")
    except Exception as e:
        print(name, "QUERY_CACHE_USED_WITH_SYSTEM_TABLE" if "QUERY_CACHE_USED_WITH_SYSTEM_TABLE" in str(e) else e)
EOF

# The catalog queries still count towards the limits of the user, like any other query.
user="user_05838_${CLICKHOUSE_DATABASE}"
query_id="05838_sleep_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user; CREATE USER $user IDENTIFIED WITH no_password SETTINGS max_concurrent_queries_for_user = 1; GRANT SELECT ON system.* TO $user"

$CLICKHOUSE_CLIENT --user "$user" --query_id "$query_id" -q "SELECT sleepEachRow(1) FROM system.numbers LIMIT 60 SETTINGS max_block_size = 1 FORMAT Null" 2>/dev/null &
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'") == 0 ]]; do sleep 0.1; done

python3 - "${CLICKHOUSE_HOST:-localhost}" "${CLICKHOUSE_PORT_ARROWFLIGHT:-9090}" "$user" <<'PYEOF'
import sys

from pyarrow import flight

client = flight.FlightClient(f"grpc://{sys.argv[1]}:{sys.argv[2]}")
opts = flight.FlightCallOptions(headers=[client.authenticate_basic_token(sys.argv[3], "")], timeout=30)

url = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetTables"
calls = {
    "GetTables": lambda: client.get_flight_info(flight.FlightDescriptor.for_command(b"\x0a" + bytes([len(url)]) + url + b"\x12\x00"), opts),
    "GetSessionOptions": lambda: list(client.do_action(flight.Action("GetSessionOptions", b""), opts)),
}

for name, call in calls.items():
    try:
        call()
        print(name, "with another query of the user OK")
    except Exception as e:
        print(name, "with another query of the user", "TOO_MANY_SIMULTANEOUS_QUERIES" if "TOO_MANY_SIMULTANEOUS_QUERIES" in str(e) else e)
PYEOF

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait
$CLICKHOUSE_CLIENT -q "DROP USER $user"
