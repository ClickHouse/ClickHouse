#!/usr/bin/env bash

# The consumer groups of the Kafka-compatible interface (`kafka_port`):
# - the grants on the table of committed offsets protect the groups from the users without them;
# - a static member (with a group instance id) that restarts replaces its previous incarnation, which is fenced;
# - a member that died without leaving the group does not block the commits of a consumer without group membership.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'password';
"

PYTHONPATH="$CUR_DIR/helpers" python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_KAFKA}" "${CLICKHOUSE_DATABASE}" "${USER_NAME}" <<'EOF'
import sys
import time

from kafka_protocol_client import Client

host, port, database, user = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]

GROUP_AUTHORIZATION_FAILED = 30
FENCED_INSTANCE_ID = 82

admin = Client(host, port)

print("Access to the groups")
group = f"{database}_access"
error, generation, leader, member = admin.join_group(group)
print("join:", error, generation, leader == member)
print("sync:", admin.sync_group(group, generation, member, assignments=[(member, b"assignment")]))

# A user without grants on the table of committed offsets.
with Client(host, port, user, "password") as client:
    error, groups = client.list_groups()
    print("list:", error, group in groups)
    print("describe:", client.describe_group(group) == (GROUP_AUTHORIZATION_FAILED, "", []))
    print("join:", client.join_group(group)[0] == GROUP_AUTHORIZATION_FAILED)
    print("sync:", client.sync_group(group, generation, member)[0] == GROUP_AUTHORIZATION_FAILED)
    print("heartbeat:", client.heartbeat(group, generation, member) == GROUP_AUTHORIZATION_FAILED)
    print("leave:", client.leave_group(group, member) == GROUP_AUTHORIZATION_FAILED)
    print("commit:", client.offset_commit(group, "topic", 0, 1, generation, member) == GROUP_AUTHORIZATION_FAILED)

# The member is still in the group.
error, groups = admin.list_groups()
print("list:", error, group in groups)
print("describe:", admin.describe_group(group) == (0, "Stable", [member]))
print("heartbeat:", admin.heartbeat(group, generation, member))
print("leave:", admin.leave_group(group, member))

print("Static membership")
group = f"{database}_static"
error, generation, leader, member = admin.join_group(group, instance_id="instance")
print("join:", error, generation, leader == member)
print("sync:", admin.sync_group(group, generation, member, "instance", [(member, b"assignment")]))

# The consumer restarts: it joins with the same instance id and without a member id.
with Client(host, port) as restarted:
    error, new_generation, new_leader, new_member = restarted.join_group(group, instance_id="instance")
    print("join after restart:", error, new_generation == generation, new_member != member, new_leader == new_member)
    print("sync after restart:", restarted.sync_group(group, new_generation, new_member, "instance"))
    print("heartbeat after restart:", restarted.heartbeat(group, new_generation, new_member, "instance"))

    # The previous incarnation is fenced.
    print("heartbeat of the previous member:", admin.heartbeat(group, generation, member, "instance") == FENCED_INSTANCE_ID)
    print("sync of the previous member:", admin.sync_group(group, generation, member, "instance")[0] == FENCED_INSTANCE_ID)
    print("join of the previous member:", admin.join_group(group, member, "instance")[0] == FENCED_INSTANCE_ID)
    print("describe:", admin.describe_group(group) == (0, "Stable", [new_member]))
    print("leave:", restarted.leave_group(group, new_member))

print("A commit without group membership after the last member died")
group = f"{database}_expired"
with Client(host, port) as dying:
    error, generation, leader, member = dying.join_group(group, session_timeout_ms=1000)
    print("join:", error, generation)

# Only the commits are sent: nothing else removes the member after its session times out.
deadline = time.monotonic() + 60
error = None
while time.monotonic() < deadline:
    error = admin.offset_commit(group, f"{database}.topic", 0, 1)
    if error == 0:
        break
    time.sleep(0.1)
print("commit:", error)
EOF

$CLICKHOUSE_CLIENT -q "
    SELECT topic = '${CLICKHOUSE_DATABASE}.topic', partition, committed_offset FROM default.__consumer_offsets FINAL WHERE group_id = '${CLICKHOUSE_DATABASE}_expired';
    DROP USER ${USER_NAME};
"
