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

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER_NAME}"
rm -rf "${TMP_DIR}"
