#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `??` command needs the AI SQL generator (`ENABLE_CLIENT_AI`), which is not built in the fast test.

# `?? <text>` asks the configured model for a query: the generator offers it the schema exploration
# tools, echoes every tool call together with the result handed back to the model, and finally says
# whether a query came out of the answer. Documented in
# docs/concepts/features/interfaces/client.mdx, section "AI-powered SQL generation".
#
# The provider is a local HTTP server speaking just enough of the OpenAI chat completion protocol,
# so the test needs neither network nor an API key, and its canned answers pick the path taken.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ai_sql"
PORT_PIPE="$WORK/port"
rm -rf "$WORK"
mkdir -p "$WORK"
mkfifo "$PORT_PIPE"

# A marker in the request picks the answer. The request carries the prompt, so the marker travels
# with it; a request that already carries tool results is the second step of the TOOLS case.
# TOOLS:  one step with three tool calls - a tool whose result holds a string, a tool whose result
#         holds no `result` field, and a tool the client does not have - then the generated query.
# EMPTY:  an answer whose `<sql>` block holds nothing but spaces.
# REJECT: `400`, which the provider client reports without retrying.
python3 - "$PORT_PIPE" <<'PYEOF' 2>/dev/null &
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def answer(content=None, tool_calls=None):
    message = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {"id": "chatcmpl-mock", "object": "chat.completion", "created": 0, "model": "mock-model",
            "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if tool_calls else "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}

def call(number, name, arguments):
    return {"id": "call_0000000%d" % number, "type": "function", "function": {"name": name, "arguments": arguments}}

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        request = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8", "replace")
        code = 200
        if '"tool_call_id"' in request:
            body = answer("<sql>SELECT 7</sql>")
        elif "MARKER_TOOLS" in request:
            body = answer(None, [call(1, "list_tables_in_database", '{"database": "no_such_db"}'),
                                 call(2, "get_schema_for_table", '{"database": "system", "table": "no_such_table"}'),
                                 call(3, "no_such_tool", "{}")])
        elif "MARKER_EMPTY" in request:
            body = answer("<sql>   </sql>")
        else:
            code = 400
            body = {"error": {"message": "rejected", "type": "invalid_request_error"}}
        out = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
server.daemon_threads = True
# Publishing the port it was given is the last thing before serving, so there is nothing to poll.
with open(sys.argv[1], "w") as pipe:
    pipe.write(str(server.server_address[1]) + "\n")
server.serve_forever()
PYEOF
MOCK_PID=$!
trap 'kill "$MOCK_PID" 2>/dev/null; wait "$MOCK_PID" 2>/dev/null; rm -rf "$WORK"' EXIT

# Read-write on the pipe, so a provider that died before publishing fails the read instead of
# blocking the open forever. An OS-assigned port keeps the copies of this test independent.
exec 3<>"$PORT_PIPE"
read -r -t 600 PORT <&3
exec 3>&-

cat > "$WORK/config.xml" <<EOF
<clickhouse>
    <ai>
        <provider>openai</provider>
        <api_key>mock</api_key>
        <base_url>http://127.0.0.1:${PORT}</base_url>
        <model>mock-model</model>
        <enable_schema_access>true</enable_schema_access>
        <max_steps>3</max_steps>
    </ai>
</clickhouse>
EOF

# The generator reports to stderr and paints the report with escape sequences and in-place line
# rewrites, so fold both streams together, break the rewritten lines apart, drop the sequences and
# drop the log lines of the provider client.
generate()
{
    ${CLICKHOUSE_LOCAL} --config-file "$WORK/config.xml" --query "?? $1" 2>&1 \
        | tr '\r' '\n' | sed -e 's/\x1b\[[0-9;]*[A-Za-z]//g' -e '/^\[/d'
}

echo 'tool calls:'
generate 'MARKER_TOOLS which tables does no_such_db have' | grep -aoE \
    "Found [0-9]+ tables in database 'no_such_db'|Could not retrieve schema for system[.]no_such_table|Tool not found|SQL query generated successfully"
echo 'empty answer:'
generate 'MARKER_EMPTY answer with nothing' | grep -aoE 'No SQL query was generated'
echo 'rejected request:'
generate 'MARKER_REJECT make the provider refuse' | grep -aoE 'AI query generation failed'
