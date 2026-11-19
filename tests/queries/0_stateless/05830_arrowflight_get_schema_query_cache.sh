#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no Arrow Flight (ENABLE_LIBRARIES=0)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Arrow Flight GetSchema builds the pipeline of the query only to read its header: it must not store an empty result in the query cache.
python3 - "${CLICKHOUSE_HOST:-localhost}" "${CLICKHOUSE_PORT_ARROWFLIGHT:-9090}" "$CLICKHOUSE_DATABASE" <<'EOF'
import sys
import uuid

from pyarrow import flight

host, port, database = sys.argv[1:4]
client = flight.FlightClient(f"grpc://{host}:{port}")
opts = flight.FlightCallOptions(headers=[client.authenticate_basic_token("default", "")], timeout=30)

tag = f"{database}_{uuid.uuid4().hex}"
query = flight.FlightDescriptor.for_command(
    f"SELECT number FROM numbers(10) SETTINGS use_query_cache = 1, query_cache_tag = '{tag}'")


def cache_entries():
    ticket = flight.Ticket(f"SELECT count() FROM system.query_cache WHERE tag = '{tag}'".encode())
    return client.do_get(ticket, opts).read_all().column(0)[0].as_py()


print(client.get_schema(query, opts).schema.names)
print("cache entries after GetSchema:", cache_entries())
info = client.get_flight_info(query, opts)
print("rows:", sum(client.do_get(endpoint.ticket, opts).read_all().num_rows for endpoint in info.endpoints))
EOF
