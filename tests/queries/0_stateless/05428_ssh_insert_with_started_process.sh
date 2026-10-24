#!/usr/bin/env bash
# Tags: no-fasttest, no-openssl-fips
# no-openssl-fips: authenticates with an ssh-ed25519 key, which is not FIPS-approved and is rejected on FIPS builds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A process started by the server (here an `executable_pool` dictionary source) must not inherit
# the pipes of an SSH session without a terminal. Otherwise the embedded client never sees the end
# of its input and an INSERT fed through `ssh` never finishes.

if ! echo "" | nc -w 1 "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_SSH}" >/dev/null 2>&1; then
    echo "@@SKIP@@: SSH port ${CLICKHOUSE_PORT_SSH} is not available, skipping test"
    exit 0
fi

if ! command -v ssh &>/dev/null; then
    echo "@@SKIP@@: ssh client is not available, skipping test"
    exit 0
fi

# OpenSSH refuses keys with permissions that are too open (644 from git).
SSH_USER_KEY_ORIG="$CURDIR/../../config/ssh_user_ed25519_key"
SSH_USER_KEY="${CLICKHOUSE_TMP}/ssh_user_ed25519_key_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cp "$SSH_USER_KEY_ORIG" "$SSH_USER_KEY"
chmod 600 "$SSH_USER_KEY"

SSH_USER_PUBKEY=$(awk '{print $2}' "$SSH_USER_KEY_ORIG.pub")
SSH_USER="ssh_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
SSH_STDERR="${CLICKHOUSE_TMP}/ssh_stderr_${CLICKHOUSE_TEST_UNIQUE_NAME}"
FIFO="${CLICKHOUSE_TMP}/ssh_stdin_${CLICKHOUSE_TEST_UNIQUE_NAME}"
MARKER="ssh_started_process_${CLICKHOUSE_DATABASE}"

SSH_OPTS=(
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o BatchMode=yes
    -o ConnectTimeout=50
    -o ServerAliveInterval=25
    -o ServerAliveCountMax=15
    -i "$SSH_USER_KEY"
    -p "${CLICKHOUSE_PORT_SSH}"
)

function count_running()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE Settings['log_comment'] = '${MARKER}'"
}

function wait_running()
{
    for _ in {1..60}; do
        running=$(count_running)
        [ "$running" = "$1" ] && break
        sleep 1
    done
}

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${SSH_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${SSH_USER} IDENTIFIED WITH ssh_key BY KEY '${SSH_USER_PUBKEY}' TYPE 'ssh-ed25519'"
${CLICKHOUSE_CLIENT} --query "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${SSH_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (x UInt64) ENGINE = Memory"

rm -f "$FIFO"
mkfifo "$FIFO"
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${CLICKHOUSE_HOST}" \
    "INSERT INTO ${CLICKHOUSE_DATABASE}.t SETTINGS log_comment = '${MARKER}' FORMAT TSV" < "$FIFO" 2>"$SSH_STDERR" &
SSH_PID=$!
exec {fifo_fd}>"$FIFO"
# The INSERT starts once its input has data.
echo 1 >&"$fifo_fd"
wait_running 1
echo "$running"

# A new pool, so that the next query starts its process while the session of the INSERT exists.
# No other parallel test reloads this dictionary.
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD DICTIONARY executable_pool_complex_implicit_key"
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('executable_pool_complex_implicit_key', 'a', ('1', '1'))"

# The end of the input must reach the INSERT although the process started above is still running.
exec {fifo_fd}>&-
wait_running 0
echo "$running"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t"

# A new pool stops the process started above, so that no session stays stuck after a failure.
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD DICTIONARY executable_pool_complex_implicit_key"
kill "$SSH_PID" 2>/dev/null
wait "$SSH_PID" 2>/dev/null
wait_running 0

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${SSH_USER}"
rm -f "$SSH_USER_KEY" "$SSH_STDERR" "$FIFO"
