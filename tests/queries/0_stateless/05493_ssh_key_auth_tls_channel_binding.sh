#!/usr/bin/env bash
# Tags: no-fasttest, no-openssl-fips
# no-fasttest: the fast test build has neither SSH key authentication nor TLS.
# no-openssl-fips: authenticates with an ssh-ed25519 key, which is not FIPS-approved and is rejected on FIPS builds.

# `ssh_key` authentication over the native protocol signs a message bound to the TLS session:
# the signature is only valid on the TLS session it was made on.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${TMP_DIR}"

ssh-keygen -q -t ed25519 -N '' -f "${TMP_DIR}/id_ed25519"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 -subj '/CN=localhost' \
    -keyout "${TMP_DIR}/relay.key" -out "${TMP_DIR}/relay.crt" 2>/dev/null

USER_NAME="ssh_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME} IDENTIFIED WITH ssh_key BY KEY '$(awk '{print $2}' "${TMP_DIR}/id_ed25519.pub")' TYPE 'ssh-ed25519';
"

# The client binary is invoked directly, because `${CLICKHOUSE_CLIENT}` already carries `--host` and `--port`,
# and the first value of a repeated option wins.
function run_client()
{
    local host=$1 port=$2
    shift 2
    ${CLICKHOUSE_CLIENT_BINARY} --host "${host}" --port "${port}" "$@" \
        --user "${USER_NAME}" --ssh-key-file "${TMP_DIR}/id_ed25519" \
        --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | grep -o -m1 -e '^1$' -e 'AUTHENTICATION_FAILED'
}

# Runs the client through a single-connection relay to the given server port.
function run_through_relay()
{
    local front=$1 backend_port=$2 back=$3
    shift 3
    local port_file="${TMP_DIR}/relay_${front}_${back}_${backend_port}.port"
    python3 "${CUR_DIR}/helpers/tls_relay.py" "${front}" "${CLICKHOUSE_HOST}" "${backend_port}" "${back}" \
        "${TMP_DIR}/relay.crt" "${TMP_DIR}/relay.key" "${port_file}" &
    local relay_pid=$!
    for _ in $(seq 1 100)
    do
        [ -s "${port_file}" ] && break
        sleep 0.1
    done
    run_client 127.0.0.1 "$(cat "${port_file}")" "$@"
    wait "${relay_pid}"
}

echo '--- plaintext port'
run_client "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP}"

echo '--- secure port'
run_client "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP_SECURE}" --secure --accept-invalid-certificate

echo '--- passthrough to the secure port: one TLS session between the client and the server'
run_through_relay raw "${CLICKHOUSE_PORT_TCP_SECURE}" raw --secure --accept-invalid-certificate

echo '--- relay with its own TLS session to the secure port'
run_through_relay tls "${CLICKHOUSE_PORT_TCP_SECURE}" tls --secure --accept-invalid-certificate

echo '--- relay from TLS to the plaintext port'
run_through_relay tls "${CLICKHOUSE_PORT_TCP}" raw --secure --accept-invalid-certificate

echo '--- TLS 1.2 with the Extended Master Secret extension (enabled by default)'
cat > "${TMP_DIR}/tls12_client.xml" <<'EOF'
<clickhouse>
    <openSSL>
        <client>
            <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1,tlsv1_3</disableProtocols>
        </client>
    </openSSL>
</clickhouse>
EOF
run_client "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP_SECURE}" --config-file "${TMP_DIR}/tls12_client.xml" --secure --accept-invalid-certificate

# The protocol-level cases use a minimal native-protocol peer over the plaintext port.
# The version inside the signed message is the protocol revision the client announces in `Hello`.
# A client of `REVISION_WITH_BINDING` or newer must sign the bound message, an older one signs the legacy message.
REVISION_WITH_BINDING=54497
REVISION_BEFORE_BINDING=54496

function run_peer()
{
    python3 "${CUR_DIR}/helpers/ssh_key_auth_peer.py" client "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP}" \
        "${USER_NAME}" "${TMP_DIR}/id_ed25519" "$@"
}

echo '--- a client of the new revision signing the bound message'
run_peer "${REVISION_WITH_BINDING}" bound "${REVISION_WITH_BINDING}"

echo '--- a client of an older revision signing the legacy message'
run_peer "${REVISION_BEFORE_BINDING}" legacy "${REVISION_BEFORE_BINDING}"

echo '--- a client of the new revision signing the legacy message, as after a relay has replaced the challenge packet'
run_peer "${REVISION_WITH_BINDING}" legacy "${REVISION_WITH_BINDING}"

echo '--- a lowered revision in Hello, while the client has signed its real revision, as after a relay has rewritten Hello'
run_peer "${REVISION_BEFORE_BINDING}" legacy "${REVISION_WITH_BINDING}"

echo '--- the client against an old server: the signature is over the exact legacy message'
PORT_FILE="${TMP_DIR}/old_server.port"
python3 "${CUR_DIR}/helpers/ssh_key_auth_peer.py" old-server "${PORT_FILE}" &
OLD_SERVER_PID=$!
for _ in $(seq 1 100)
do
    [ -s "${PORT_FILE}" ] && break
    sleep 0.1
done
${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "$(cat "${PORT_FILE}")" \
    --user "${USER_NAME}" --ssh-key-file "${TMP_DIR}/id_ed25519" --query "SELECT 1" > /dev/null 2>&1
wait "${OLD_SERVER_PID}"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER_NAME}"
rm -rf "${TMP_DIR}"
