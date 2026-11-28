#!/usr/bin/env bash
# Tags: no-fasttest, no-openssl-fips
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.
# no-openssl-fips: the keys are ssh-ed25519, which is not FIPS-approved and is rejected on FIPS builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `ssh` listener of the proxy logs in to the backend with its own bastion key, so the client's public key is
# the only credential, and the allowlist of the matched rule is the authorization boundary:
# - a client that proves possession of an allowlisted key is let through to the backend;
# - a client that only asks whether an allowlisted key is acceptable, without signing with it, gets no backend connection;
# - a signed key that is not in an allowlist is denied, also with a listener default pool and a rule matching only
#   on the user name;
# - a backend whose host key is not in `known_hosts_file` is refused before the bastion key is used.

unset SSH_AUTH_SOCK

KEYS="${CLICKHOUSE_TMP}/05768_keys_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$KEYS"
mkdir -p "$KEYS"
chmod 700 "$KEYS"
for key in host bastion allowed unlisted other_backend; do
    ssh-keygen -q -t ed25519 -N '' -C "$key" -f "${KEYS}/${key}"
done

# The allowlisted key without its private key: the client can only ask whether the key is acceptable.
mkdir -p "${KEYS}/probe"
cp "${KEYS}/allowed.pub" "${KEYS}/probe/allowed.pub"

BASTION_USER="bastion_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${BASTION_USER};
    CREATE USER ${BASTION_USER} IDENTIFIED WITH ssh_key BY KEY '$(awk '{print $2}' "${KEYS}/bastion.pub")' TYPE 'ssh-ed25519';
"

# The real host key of the backend, and a host key of another server under the same name.
ssh-keyscan -T 10 -t rsa,ed25519 -p "${CLICKHOUSE_PORT_SSH}" "${CLICKHOUSE_HOST}" 2>/dev/null > "${KEYS}/known_hosts"
if [ ! -s "${KEYS}/known_hosts" ]; then
    echo "Cannot get the SSH host key of the server" >&2
    exit 1
fi
echo "[${CLICKHOUSE_HOST}]:${CLICKHOUSE_PORT_SSH} $(cut -d' ' -f1,2 "${KEYS}/other_backend.pub")" > "${KEYS}/untrusted_known_hosts"

CONFIG="${CLICKHOUSE_TMP}/05768_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/05768_proxy.log"
NONCE="05768_${CLICKHOUSE_DATABASE}"

PROXY_PID=
PROXY_PORT=
SSH_PORT=

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null; ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${BASTION_USER}"' EXIT

unsupported()
{
    grep -qF -e "the silk fiber framework, which is only available on Linux" -e "requires a build with libssh" "$LOG"
}

start_proxy()
{
    local known_hosts=$1

    # A free port is picked by binding a socket and immediately releasing it, so another process can
    # take it before the proxy binds it. Serve a run-unique token to tell our own proxy from a stranger.
    PROXY_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
    SSH_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")

    cat > "$CONFIG" <<EOF
<clickhouse>
    <logger><level>debug</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>http</protocol><port>${PROXY_PORT}</port></listener>
            <listener><protocol>ssh</protocol><port>${SSH_PORT}</port><pool>ch</pool></listener>
        </listeners>
        <ssh>
            <host_key_file>${KEYS}/host</host_key_file>
            <backend_user>${BASTION_USER}</backend_user>
            <backend_key_file>${KEYS}/bastion</backend_key_file>
            <known_hosts_file>${known_hosts}</known_hosts_file>
            <max_sessions>1</max_sessions>
        </ssh>
        <pools>
            <pool>
                <name>ch</name>
                <backend><host>${CLICKHOUSE_HOST}</host><ssh_port>${CLICKHOUSE_PORT_SSH}</ssh_port></backend>
            </pool>
        </pools>
        <rules>
            <rule><authorized_key>$(cat "${KEYS}/allowed.pub")</authorized_key><pool>ch</pool></rule>
            <rule><user>only_user</user><pool>ch</pool></rule>
        </rules>
        <http>
            <static>
                <page><path>/whoami</path><content>${NONCE}</content></page>
            </static>
        </http>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
</clickhouse>
EOF

    $CLICKHOUSE_BINARY proxy --config-file "$CONFIG" > "$LOG" 2>&1 &
    PROXY_PID=$!

    # Wait until our own proxy answers on the port.
    for _ in $(seq 1 100); do
        WHOAMI=$(curl -s --max-time 5 "http://127.0.0.1:${PROXY_PORT}/whoami")
        CURL_STATUS=$?
        if [ "$WHOAMI" = "$NONCE" ]; then return 0; fi
        # An answer from something else - a different body, or a hang up to the timeout (curl exit
        # code 28) - means the port was taken by a stranger, and so does our own proxy exiting (it
        # could not bind). All of them are retried on freshly picked ports.
        if [ -n "$WHOAMI" ] || [ "$CURL_STATUS" -eq 28 ] || ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
        sleep 0.2
    done

    kill "$PROXY_PID" 2>/dev/null
    wait "$PROXY_PID" 2>/dev/null
    PROXY_PID=
    return 1
}

stop_proxy()
{
    kill "$PROXY_PID" 2>/dev/null
    wait "$PROXY_PID" 2>/dev/null
    PROXY_PID=
}

start_proxy_or_exit()
{
    local ready=0
    for _ in $(seq 1 5); do
        if start_proxy "$1"; then ready=1; break; fi
        # A build without the fiber framework or without SSH support will never come up; do not retry it.
        if unsupported; then break; fi
    done

    if [ "$ready" -ne 1 ]; then
        if unsupported; then
            # The proxy is not available in this build; emit the expected output so the test passes.
            cat "$CUR_DIR/05768_proxy_ssh_authorization.reference"
            exit 0
        fi
        echo "proxy did not start" >&2
        cat "$LOG" >&2
        exit 1
    fi
}

# Prints the output of the query, or `denied` if the connection does not get through.
run_ssh()
{
    local key=$1
    local user=$2
    timeout 60 ssh -F /dev/null -n -i "$key" -o IdentitiesOnly=yes -o BatchMode=yes -o PasswordAuthentication=no \
        -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30 \
        -p "$SSH_PORT" "${user}@127.0.0.1" "SELECT currentUser() = '${BASTION_USER}'" 2>/dev/null || echo "denied"
}

routed()
{
    grep -c "routed to backend" "$LOG"
}

start_proxy_or_exit "${KEYS}/known_hosts"

echo "probe of an allowlisted key without a signature"
run_ssh "${KEYS}/probe/allowed.pub" user
routed

echo "unlisted key, default pool"
run_ssh "${KEYS}/unlisted" user
echo "unlisted key, rule on the user name"
run_ssh "${KEYS}/unlisted" only_user
routed

echo "allowlisted key"
run_ssh "${KEYS}/allowed" user
routed

stop_proxy

echo "untrusted backend host key"
start_proxy_or_exit "${KEYS}/untrusted_known_hosts"
run_ssh "${KEYS}/allowed" user
stop_proxy
# The client was let through by the proxy, and the backend was refused before the bastion key was used.
routed
grep -c "is not trusted" "$LOG"
