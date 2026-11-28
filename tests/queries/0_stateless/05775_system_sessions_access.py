#!/usr/bin/env python3
# Tags: no-fasttest
# no-fasttest: requires clickhouse_driver, like the other persistent native-connection tests.

# Regressions for `system.sessions`:
# 1. A role granted to an already enabled role after the login invalidates the memoised quota:
#    the first query must check the newly enabled role's client-key requirement.
# 2. A user without the `SHOW CONNECTIONS` privilege sees only the sessions of this user. The
#    visibility follows the user ID: a renamed user keeps seeing the own session, whose login-time
#    name stays until the next authentication.

import os
import urllib.parse
import urllib.request
import uuid

from clickhouse_driver import Client, errors

QUOTA_REQUIRES_CLIENT_KEY = 496

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT_TCP = int(os.environ.get("CLICKHOUSE_PORT_TCP", "9000"))
CLICKHOUSE_PORT_HTTP = int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}/"

suffix = uuid.uuid4().hex
quota_user = "session_quota_user_" + suffix
outer_role = "session_quota_outer_" + suffix
quota_role = "session_quota_inner_" + suffix
quota = "session_quota_" + suffix
alice = "alice_" + CLICKHOUSE_DATABASE
bob = "bob_" + CLICKHOUSE_DATABASE

def http_query(sql, user="default"):
    url = HTTP_URL + "?" + urllib.parse.urlencode({"user": user})
    with urllib.request.urlopen(url, data=sql.encode(), timeout=60) as response:
        return response.read().decode().rstrip("\n")

admin = Client(CLICKHOUSE_HOST, port=CLICKHOUSE_PORT_TCP, database="")
quota_client = None
alice_client = None

try:
    database = admin.execute("SELECT currentDatabase()")[0][0]

    admin.execute(f"CREATE ROLE {outer_role}, {quota_role}")
    admin.execute(f"CREATE USER {quota_user}")
    admin.execute(f"GRANT SELECT ON *.* TO {outer_role}")
    admin.execute(f"GRANT {outer_role} TO {quota_user}")
    admin.execute(f"SET DEFAULT ROLE {outer_role} TO {quota_user}")
    admin.execute(f"CREATE QUOTA {quota} KEYED BY client_key FOR INTERVAL 1 HOUR MAX queries = 100 TO {quota_role}")

    print("--- role grant after login invalidates the memoised quota")
    # Only authenticate here. An ordinary query before the grant would hide the login-induced regression.
    quota_client = Client(CLICKHOUSE_HOST, port=CLICKHOUSE_PORT_TCP, database=database, user=quota_user)
    quota_client.connection.connect()
    # Access-control notifications are delivered before `GRANT` returns, so no timed wait is needed.
    admin.execute(f"GRANT {quota_role} TO {outer_role}")
    try:
        quota_client.execute("SELECT number FROM numbers(1)")
    except errors.ServerException as exception:
        assert exception.code == QUOTA_REQUIRES_CLIENT_KEY, exception
    else:
        raise AssertionError("The first query bypassed the newly enabled role's quota key requirement")
    print("first query checks the newly enabled role quota")
    quota_client.disconnect()
    quota_client = None

    admin.execute(f"CREATE USER {alice} NOT IDENTIFIED")
    admin.execute(f"CREATE USER {bob} NOT IDENTIFIED")
    # The explicit `SELECT` keeps this test valid with either value of
    # `access_control_improvements.select_from_system_db_requires_grant`.
    admin.execute(f"GRANT SELECT ON system.sessions TO {alice}, {bob}")

    # A session of a different user. This connection stays open and idle until the checks end.
    alice_client = Client(CLICKHOUSE_HOST, port=CLICKHOUSE_PORT_TCP, user=alice)
    other_auth_id = alice_client.execute("SELECT auth_id FROM system.sessions WHERE user = currentUser()")[0][0]

    print("--- without SHOW CONNECTIONS: only the sessions of this user")
    print(http_query(f"SELECT count() FROM system.sessions WHERE auth_id = '{other_auth_id}'", bob))
    # The own session of this request is visible without any extra privilege.
    print(http_query("SELECT countIf(user = currentUser()) > 0 FROM system.sessions", bob))

    admin.execute(f"GRANT SHOW CONNECTIONS ON *.* TO {bob}")

    print("--- with SHOW CONNECTIONS: sessions of all users")
    print(http_query(f"SELECT user FROM system.sessions WHERE auth_id = '{other_auth_id}'", bob))

    # The visibility follows the user ID: the renamed user still sees the own session, and the
    # login-time name of its entry stays until the next authentication.
    admin.execute(f"ALTER USER {alice} RENAME TO {alice}_renamed")
    print("--- after renaming the other user")
    print(http_query(f"SELECT user FROM system.sessions WHERE auth_id = '{other_auth_id}'", bob))
    print(alice_client.execute(f"SELECT count() FROM system.sessions WHERE user = '{alice}'")[0][0])

    admin.execute(f"REVOKE SHOW CONNECTIONS ON *.* FROM {bob}")

    print("--- after revoking SHOW CONNECTIONS")
    print(http_query(f"SELECT count() FROM system.sessions WHERE auth_id = '{other_auth_id}'", bob))
finally:
    if quota_client is not None:
        quota_client.disconnect()
    if alice_client is not None:
        alice_client.disconnect()
    admin.execute(f"DROP USER IF EXISTS {quota_user}, {alice}, {alice}_renamed, {bob}")
    admin.execute(f"DROP QUOTA IF EXISTS {quota}")
    admin.execute(f"DROP ROLE IF EXISTS {outer_role}, {quota_role}")
    admin.disconnect()
