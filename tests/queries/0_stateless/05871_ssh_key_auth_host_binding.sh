#!/usr/bin/env bash
# Tags: no-fasttest, no-openssl-fips
# no-fasttest: the fast test build has neither SSH key authentication nor TLS.
# no-openssl-fips: authenticates with an ssh-ed25519 key, which is not FIPS-approved and is rejected on FIPS builds.

# `ssh_key` authentication over the native protocol signs the name of the host the client connects to,
# and the server accepts the login only if that name is in `ssh_key_auth.accepted_hosts`
# (`tests/config/config.d/ssh_key_auth.xml`: `localhost`, `127.0.0.1`, `::1`, and `ssh-key-auth-test-[0-9]+\.example\.com`).

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
# The client connects to the relay by the address it listens on, so that is the name the client signs.
function run_through_relay()
{
    local front=$1 backend_port=$2 back=$3 listen_address=$4
    shift 4
    local port_file="${TMP_DIR}/relay_${front}_${back}_${backend_port}_${listen_address}.port"
    python3 "${CUR_DIR}/helpers/tls_relay.py" "${front}" "${CLICKHOUSE_HOST}" "${backend_port}" "${back}" \
        "${TMP_DIR}/relay.crt" "${TMP_DIR}/relay.key" "${port_file}" "${listen_address}" &
    local relay_pid=$!
    for _ in $(seq 1 100)
    do
        [ -s "${port_file}" ] && break
        sleep 0.1
    done
    run_client "${listen_address}" "$(cat "${port_file}")" "$@"
    wait "${relay_pid}"
}

echo '--- plaintext port'
run_client "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP}"

echo '--- secure port'
run_client "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP_SECURE}" --secure --accept-invalid-certificate

echo '--- TLS terminated in front of the plaintext port, at a name the server accepts (a load balancer)'
run_through_relay tls "${CLICKHOUSE_PORT_TCP}" raw 127.0.0.1 --secure --accept-invalid-certificate

echo '--- TLS terminated and opened again in front of the secure port, at a name the server accepts'
run_through_relay tls "${CLICKHOUSE_PORT_TCP_SECURE}" tls 127.0.0.1 --secure --accept-invalid-certificate

echo '--- relay at a name the server does not accept'
run_through_relay raw "${CLICKHOUSE_PORT_TCP}" raw 127.0.0.2

# The protocol-level cases use a minimal native-protocol peer over the plaintext port.
# The version inside the signed message is the protocol revision the client announces in `Hello`.
# A client of `REVISION_WITH_HOST` or newer must sign the message with the host name, an older one signs the legacy message.
REVISION_WITH_HOST=54498
REVISION_BEFORE_HOST=54497

function run_peer()
{
    python3 "${CUR_DIR}/helpers/ssh_key_auth_peer.py" client "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP}" \
        "${USER_NAME}" "${TMP_DIR}/id_ed25519" "$@"
}

echo '--- a listed name'
run_peer "${REVISION_WITH_HOST}" host "${REVISION_WITH_HOST}" localhost

echo '--- a name matching the regular expression, in upper case and with a trailing dot'
run_peer "${REVISION_WITH_HOST}" host "${REVISION_WITH_HOST}" SSH-KEY-AUTH-TEST-7.EXAMPLE.COM.

echo '--- a name that the regular expression matches only in part'
run_peer "${REVISION_WITH_HOST}" host "${REVISION_WITH_HOST}" ssh-key-auth-test-7.example.com.evil.com

echo '--- a name the server does not accept'
run_peer "${REVISION_WITH_HOST}" host "${REVISION_WITH_HOST}" evil.example.com

echo '--- a listed name sent next to a signature over another name, as after a relay has replaced the name'
run_peer "${REVISION_WITH_HOST}" host "${REVISION_WITH_HOST}" evil.example.com localhost

echo '--- a client of an older revision signing the legacy message'
run_peer "${REVISION_BEFORE_HOST}" legacy "${REVISION_BEFORE_HOST}"

echo '--- a client of the new revision signing the legacy message, as after a relay has replaced the challenge packet'
run_peer "${REVISION_WITH_HOST}" legacy "${REVISION_WITH_HOST}"

echo '--- a lowered revision in Hello, while the client has signed its real revision, as after a relay has rewritten Hello'
run_peer "${REVISION_BEFORE_HOST}" legacy "${REVISION_WITH_HOST}"

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
