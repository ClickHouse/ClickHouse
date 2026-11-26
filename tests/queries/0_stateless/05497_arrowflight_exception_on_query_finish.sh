#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no Arrow Flight (ENABLE_LIBRARIES=0)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Arrow Flight calls in a session with memory-tracker fault injection: an exception thrown while the query
# is being finished must become an error status of the call, not terminate the server. The fault lands at
# a random allocation, so loop until it has hit the finishing step many times.
python3 - "${CLICKHOUSE_HOST:-localhost}" "${CLICKHOUSE_PORT_ARROWFLIGHT:-9090}" <<'EOF'
import sys
import time
import uuid

import pyarrow as pa
from pyarrow import flight

client = flight.FlightClient(f"grpc://{sys.argv[1]}:{sys.argv[2]}")
token = client.authenticate_basic_token("default", "")


def options():
    return flight.FlightCallOptions(headers=[token, (b"x-clickhouse-session-id", str(uuid.uuid4()).encode())], timeout=30)


opts = options()
query = flight.FlightDescriptor.for_command("SELECT 1")
writer, _ = client.do_put(
    flight.FlightDescriptor.for_command("SET memory_tracker_fault_probability = 0.002, max_untracked_memory = 0"),
    pa.schema([]), opts)
writer.close()

calls = [
    lambda: client.do_put(query, pa.schema([]), opts)[0].close(),
    lambda: client.do_get(flight.Ticket(b"SELECT 1"), opts).read_all(),
    lambda: client.get_flight_info(query, opts),
    lambda: client.get_schema(query, opts),
]

ok = 0
faults = 0
start = time.monotonic()
for i in range(200):
    if time.monotonic() - start > 30:
        break
    for call in calls:
        try:
            call()
            ok += 1
        except Exception as e:  # get_schema raises OSError, the other calls raise FlightError subclasses
            if "fault injected" in str(e):
                faults += 1
            elif isinstance(e, flight.FlightUnavailableError) or "unavailable error" in str(e):
                print(f"DIED after {i} iterations: {e}")
                sys.exit(0)

print(int(faults > 0))
print(int(ok > 0))
opts = options()
print(client.do_get(flight.Ticket(b"SELECT 42"), opts).read_all().column(0).to_pylist()[0])
EOF

$CLICKHOUSE_CLIENT -q "SELECT 'alive'"
