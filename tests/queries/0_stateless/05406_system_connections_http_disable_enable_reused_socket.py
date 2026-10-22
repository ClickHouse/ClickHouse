#!/usr/bin/env python3
# Tags: no-parallel
# no-parallel: the test sets `collect_connection_metrics` to `false` for a short time. Other tests must not see that.

# An HTTP keep-alive connection that is registered before `collect_connection_metrics` changes to `false`
# must have the client properties of its last request when the setting changes to `true` again.
# 1. `alice` runs a query on the socket. The connection gets its registration.
# 2. The setting changes to `false`. `bob` runs a query on the same socket.
# 3. The setting changes to `true`. The row must show `bob`. `bob` sees the row and `alice` does not.

import http.client
import os
import threading
import urllib.parse
import urllib.request

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT_HTTP = int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
# The same default as in `shell_config.sh`.
CLICKHOUSE_CONFIG_DIR = os.environ.get("CLICKHOUSE_CONFIG_DIR", "/etc/clickhouse-server")
HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}/"

ALICE = f"alice_{CLICKHOUSE_DATABASE}"
BOB = f"bob_{CLICKHOUSE_DATABASE}"
OWN_ID_QUERY = "SELECT connection_id FROM system.connections WHERE query_id = currentQueryID()"

# The files in `config.d` are merged in alphabetical order. Thus, this file overrides `collect_connection_metrics.xml`.
OVERRIDE_PATH = os.path.join(CLICKHOUSE_CONFIG_DIR, "config.d", "zz_05406_disable_collect_connection_metrics.xml")


def query(sql, user="default"):
    url = HTTP_URL + "?" + urllib.parse.urlencode({"user": user})
    with urllib.request.urlopen(url, data=sql.encode(), timeout=60) as response:
        return response.read().decode().rstrip("\n")


def set_collection(enabled):
    if enabled:
        if os.path.exists(OVERRIDE_PATH):
            os.remove(OVERRIDE_PATH)
    else:
        with open(OVERRIDE_PATH, "w") as f:
            f.write("<clickhouse><collect_connection_metrics>false</collect_connection_metrics></clickhouse>\n")
    query("SYSTEM RELOAD CONFIG")
    return query("SELECT value FROM system.server_settings WHERE name = 'collect_connection_metrics'")


connection = http.client.HTTPConnection(CLICKHOUSE_HOST, CLICKHOUSE_PORT_HTTP, timeout=60)


def keep_alive_request(method, path, body=None, user_agent=""):
    connection.request(method, path, body=body, headers={"User-Agent": user_agent})
    response = connection.getresponse()
    data = response.read().decode().strip()
    assert response.status == 200, (response.status, data)
    assert not response.will_close, "the server closed the keep-alive connection"
    return data


def keep_alive_query(sql, user, user_agent):
    return keep_alive_request("POST", "/?" + urllib.parse.urlencode({"user": user}), sql, user_agent)


def set_collection_keeping_socket_alive(enabled):
    # A reload can take longer than `keep_alive_timeout` on a slow build. Then the server closes the idle socket.
    # Thus, send `/ping` on the socket until the reload ends. `/ping` does not change the registration of the connection.
    result = {}
    reload_thread = threading.Thread(target=lambda: result.update(value=set_collection(enabled)))
    reload_thread.start()
    while reload_thread.is_alive():
        keep_alive_request("GET", "/ping")
        reload_thread.join(timeout=1)
    return result["value"]


for name in (ALICE, BOB):
    query(f"DROP USER IF EXISTS {name}")
    query(f"CREATE USER {name} NOT IDENTIFIED")
    query(f"GRANT SELECT ON system.connections TO {name}")

try:
    connection_id = keep_alive_query(OWN_ID_QUERY, ALICE, "alice-agent")

    print("--- collection disabled:", set_collection_keeping_socket_alive(False))
    keep_alive_query("SELECT 1", BOB, "bob-agent")

    print("--- collection enabled:", set_collection_keeping_socket_alive(True))
    print(query(f"SELECT user = '{BOB}', client_name FROM system.connections WHERE connection_id = '{connection_id}'"))
    print("bob sees the connection:", query(f"SELECT count() FROM system.connections WHERE connection_id = '{connection_id}'", BOB))
    print("alice sees the connection:", query(f"SELECT count() FROM system.connections WHERE connection_id = '{connection_id}'", ALICE))

    connection.close()
finally:
    set_collection(True)
    for name in (ALICE, BOB):
        query(f"DROP USER IF EXISTS {name}")
