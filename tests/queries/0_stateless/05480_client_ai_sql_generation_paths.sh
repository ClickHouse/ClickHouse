#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `??` command needs the AI SQL generator (`ENABLE_CLIENT_AI`), which is not built in the fast test.

# `?? <text>` asks the configured model for a query: the generator echoes every call of the schema
# exploration tools with the result handed back to the model, says whether a query came out of the
# answer, and puts that query into the next input line, where the user runs it. Documented in
# docs/concepts/features/interfaces/client.mdx, "AI-powered SQL generation". The provider is a
# local HTTP server speaking just enough of the OpenAI chat completion protocol, so no network and
# no API key are needed; all its answers are successful ones, because a provider that refuses makes
# the generator report the refusal as a `LOGICAL_ERROR`, which aborts a build with assertions. The
# query reaches the input line only in an interactive session, so the client runs on a pseudo
# terminal, in one process for both cases, as the coverage binary is slow to start.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ai_sql"
rm -rf "$WORK" && mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT
export WORK

python3 - <<'PYEOF'
import json, os, pty, re, select, shlex, signal, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
WORK = os.environ["WORK"]
def answer(content=None, calls=None):
    message = {"role": "assistant", "content": content}
    if calls:
        message["tool_calls"] = [{"id": "call_000000%d" % i, "type": "function",
                                  "function": {"name": n, "arguments": a}} for i, (n, a) in enumerate(calls)]
    return {"id": "chatcmpl-mock", "object": "chat.completion", "created": 0, "model": "mock-model",
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if calls else "stop"}]}
# MARKER_TOOLS gets three tool calls - a result holding a string, a result without a `result` field,
# and a tool the client does not have - then the query. Anything else gets a `<sql>` block of spaces.
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        request = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8", "replace")
        if '"tool_call_id"' in request:
            body = answer("<sql>SELECT 7</sql>")
        elif "MARKER_TOOLS" in request:
            body = answer(None, [("list_tables_in_database", '{"database": "no_such_db"}'),
                                 ("get_schema_for_table", '{"database": "system", "table": "no_such_table"}'),
                                 ("no_such_tool", "{}")])
        else:
            body = answer("<sql>   </sql>")
        out = json.dumps(body).encode()
        self.send_response(200)
        [self.send_header(h, v) for h, v in (("Content-Type", "application/json"), ("Content-Length", str(len(out))))]
        self.end_headers()
        self.wfile.write(out)
    def log_message(self, *args):
        pass
server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)  # an OS-assigned port keeps the copies apart
server.daemon_threads = True
threading.Thread(target=server.serve_forever, daemon=True).start()
open(WORK + "/config.xml", "w").write("<clickhouse><ai><provider>openai</provider><api_key>mock</api_key>"
    "<model>mock-model</model><base_url>http://127.0.0.1:%d</base_url><enable_schema_access>true</enable_schema_access>"
    "<max_steps>3</max_steps></ai></clickhouse>" % server.server_address[1])
command = shlex.split(os.environ["CLICKHOUSE_LOCAL"])
pid, master = pty.fork()
if pid == 0:
    os.environ["TERM"] = "xterm"
    os.execv(command[0], command + ["--format=TSV", "--config-file=" + WORK + "/config.xml", "--history_file=" + WORK + "/history"])
ESCAPES = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b[=>]")
transcript, position = "", 0
# Read until the pattern shows up behind everything matched so far, so no expected text is answered
# by output that was already there.
def wait_for(pattern, seconds=120):
    global transcript, position
    deadline = time.monotonic() + seconds
    while True:
        match = re.compile(pattern).search(ESCAPES.sub("", transcript).replace("\r", "\n"), position)
        if match:
            position = match.end()
            return True
        if time.monotonic() > deadline:
            return False
        if select.select([master], [], [], 0.3)[0]:
            try:
                chunk = os.read(master, 65536)
            except OSError:
                return False
            if not chunk:
                return False
            transcript += chunk.decode(errors="replace")
PROMPT = r"\:\) "
# The generated query must reach the next input line as a whole - nothing of it missing and nothing
# else beside it - and running it then shows its result. Every expected text is printed when it
# shows up, so a case that does not happen loses its lines; the empty text only waits for the prompt,
# so that the next keystrokes are not typed into a running query.
PATTERN = {"prepopulated query: SELECT 7": PROMPT + r"SELECT 7(?![^\n])", "7": r"(?m)^7$", "": PROMPT}
ACTIONS = [(b"?? MARKER_TOOLS which tables does no_such_db have\r", "Found 0 tables in database 'no_such_db'"),
           (None, "Could not retrieve schema for system.no_such_table"), (None, "Tool not found"),
           (None, "SQL query generated successfully"), (None, "prepopulated query: SELECT 7"),
           (b"\r", "7"), (None, ""),
           (b"?? MARKER_EMPTY answer with nothing\r", "No SQL query was generated")]
ok = wait_for(PROMPT)
for keys, text in ACTIONS:
    if not ok:
        break
    if keys:
        os.write(master, keys)
    ok = wait_for(PATTERN.get(text, re.escape(text)))
    if ok and text:
        print(text)
os.write(master, b"\x04")
if not wait_for(r"Bye", 60) or not ok:
    ok = False
    os.kill(pid, signal.SIGKILL)
status = os.waitpid(pid, 0)[1]
# The expected lines say nothing about how the client ended, and a diagnostic of a sanitizer or of a
# failing shutdown comes behind them, so keep the status and the whole terminal for a failure.
if not ok or status != 0:
    sys.stderr.write("client wait status %d, expected text seen: %s\nterminal:\n%s\n" % (status, ok, transcript))
    sys.exit(1)
PYEOF
