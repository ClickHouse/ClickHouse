#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: on macOS, reading a terminal whose other end is closed returns EOF instead of failing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client asks whether to output a binary format into the terminal. If reading the answer fails because the
# terminal on stdin is closed, the next blocks of the result must not ask again: the input cannot be read anymore.
# The answer is the default one, so the result is not printed.
# Then, with `--ignore-error`, a next query that reads its data from stdin must not read it again either.

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_LOCAL" <<'PYEOF'
import os, select, shlex, subprocess, sys, time

CLIENT = sys.argv[1]
LOCAL = sys.argv[2]
QUESTION = b'Do you want to output it anyway?'
QUERY = "SELECT concat('result', 'payload') FROM numbers(100) SETTINGS max_block_size = 10 FORMAT RowBinary"
PAYLOAD = b'resultpayload'

def read_output(fd, output, until, timeout):
    deadline = time.monotonic() + timeout
    while until is None or until not in output:
        left = deadline - time.monotonic()
        if left <= 0:
            break
        ready, _, _ = select.select([fd], [], [], left)
        if not ready:
            continue
        try:
            chunk = os.read(fd, 65536)
        except OSError:  # The client has exited and closed the terminal.
            break
        if not chunk:
            break
        output += chunk
    return output

def run_client(command):
    # Separate terminals for stdin and stdout: closing the one on stdin makes reading the answer fail,
    # while the client can still write to stdout.
    in_master, in_slave = os.openpty()
    out_master, out_slave = os.openpty()
    client = subprocess.Popen(['sh', '-c', command],
                              stdin=in_slave, stdout=out_slave, stderr=out_slave, start_new_session=True)
    os.close(in_slave)
    os.close(out_slave)
    output = read_output(out_master, b'', QUESTION, 60)
    os.close(in_master)
    output = read_output(out_master, output, None, 60)
    code = client.wait(timeout=60)
    os.close(out_master)
    return output, code

# A client that starts reading the answer only after the terminal is closed reads EOF (the default answer)
# instead of failing, and exits with 0. Such a run does not reach the failed read, so repeat it.
for _ in range(10):
    output, code = run_client(CLIENT + ' --query ' + shlex.quote(QUERY))
    if code != 0:
        break
print(output.count(QUESTION))
print(output.count(PAYLOAD))
print(code)

# `clickhouse-local` reads the data of `input()` itself, the client sends it.
INSERTS = [(CLIENT, "INSERT INTO FUNCTION null('s String') FORMAT CSV"),
           (LOCAL, "INSERT INTO FUNCTION null('s String') SELECT * FROM input('s String') FORMAT CSV")]
for binary, insert in INSERTS:
    for _ in range(10):
        queries = [QUERY, insert, "SELECT concat('next', 'query')"]
        output, code = run_client(binary + ' --ignore-error' + ''.join(' --query ' + shlex.quote(q) for q in queries))
        failed_read = b'CANNOT_READ_FROM_FILE_DESCRIPTOR' in output
        if failed_read:
            break
    print(int(failed_read), output.count(b'nextquery'))
PYEOF
