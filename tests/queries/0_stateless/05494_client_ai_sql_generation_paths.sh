#!/usr/bin/env bash
# Tags: no-fasttest, no-tsan
# Tag no-fasttest: the `??` command needs the AI SQL generator (`ENABLE_CLIENT_AI`), which is not built in the fast test.
# Tag no-tsan: the tool call callbacks print from the main thread while the thinking animation of `AIToolExecutionDisplay::startThinking` prints to the same `std::ostream` from its own thread, so a tool call always races on the stream state (read `ios_base::width` at `AISQLGenerator.cpp:47` against the animation thread).

# `?? <text>` asks the configured model for a query: the generator echoes every call of the schema exploration tools with the
# result handed back to the model, says whether a query came out of the answer, and puts that query into the next input line, where
# the user runs it. Documented in docs/concepts/features/interfaces/client.mdx, "AI-powered SQL generation". The provider is a local
# HTTP server speaking just enough of the OpenAI chat completion protocol, so no network and no API key are needed; every answer is
# a successful one, because a refusal is reported as a `LOGICAL_ERROR`, which aborts a build with assertions. The query reaches the
# input line only in an interactive session, so the client runs on a pseudo terminal, in one process for both cases (slow binaries).

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
# by output that was already there. A pattern of `None` instead drains the terminal until it closes.
def wait_for(pattern, seconds=120):
    global transcript, position
    deadline = time.monotonic() + seconds
    while True:
        match = pattern is not None and re.compile(pattern).search(ESCAPES.sub("", transcript).replace("\r", "\n"), position)
        if match:
            position = match.end()
            return True
        if time.monotonic() > deadline:
            return False
        if select.select([master], [], [], 0.3)[0]:
            try:
                chunk = os.read(master, 65536)
            except OSError:
                return pattern is None
            if not chunk:
                return pattern is None
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
# The end-of-file character must land on an idle input line: while the client works, the terminal is in cooked mode, where it ends the current
# line instead of reaching the client, so a slow binary loses it and has to be asked again. The farewell is a holiday greeting on some days
# (`isNewYearMode`), so the session ends when the terminal closes and how it ended is the exit status alone; keep the whole terminal for that.
ended = False
for _ in range(4 if ok and wait_for(PROMPT) else 0):
    os.write(master, b"\x04")
    if wait_for(None, 30): ended = True; break
if not ended or not ok: os.kill(pid, signal.SIGKILL)
status = os.waitpid(pid, 0)[1]
if not ok or not ended or status != 0:
    sys.stderr.write("client wait status %d, text seen %s, session ended %s\nterminal:\n%s\n" % (status, ok, ended, transcript))
    sys.exit(1)
PYEOF
