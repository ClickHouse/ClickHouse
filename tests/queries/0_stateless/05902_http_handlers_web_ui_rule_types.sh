#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `acme` handler type exists only with `USE_SSL`, and the fast test build has no SSL

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `<rule>` of `<http_handlers>` whose `<handler><type>` names one of the built-in web UI pages
# (`index`, `jemalloc`, `schema`, `docs`) serves that page at the configured url. The page must be
# the very one that `<defaults/>` serves at the built-in path (`/`, `/jemalloc`, `/schema`,
# `/docs`): the expected behaviour of these rule types is defined by
# `addCommonDefaultHandlersFactory` in src/Server/HTTPHandlerFactory.cpp, which registers the same
# handler classes on those paths. A url that no rule claims still answers, with 404.
#
# The section also carries a rule of type `acme`, accepted only in an SSL-enabled build. Its url is
# restricted to GET and the test sends it a HEAD: the rule's own method filter rejects that request
# and no other rule claims the url either, so the answer is 404. The url is deliberately never
# requested with GET -- `ACME::Client::requestChallenge` (src/Server/ACME/Client.cpp) waits one
# second per retry, ten retries, as long as no account key has been loaded, which is by design but
# longer than this test may take. That the rule was accepted at all is visible in the other results:
# an unknown `<handler><type>` makes the whole factory throw `INVALID_CONFIG_PARAMETER`, and then no
# url of this section would answer.
#
# The handler factory is built while the HTTP listener is bound, so the section is reached through
# `--config-file` plus `SYSTEM START LISTEN HTTP`, as in 04935_local_http_handler_config_error.sh.
# `http_port` is 0 (OS-assigned) behind one explicit `listen_host`, so concurrent copies of this
# test cannot collide on a port.

PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
CONFIG="${PREFIX}.config.xml"
LOGS="${PREFIX}.logs"
KEEP="${PREFIX}.keep"
BODY="${PREFIX}.body"
DEFAULT_BODY="${PREFIX}.default_body"

cat > "$CONFIG" <<'XML'
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <http_port>0</http_port>
    <http_handlers>
        <rule><methods>GET</methods><url>/ui_index</url><handler><type>index</type></handler></rule>
        <rule><methods>GET</methods><url>/ui_jemalloc</url><handler><type>jemalloc</type></handler></rule>
        <rule><methods>GET</methods><url>/ui_schema</url><handler><type>schema</type></handler></rule>
        <rule><methods>GET</methods><url>/ui_docs</url><handler><type>docs</type></handler></rule>
        <rule><methods>GET</methods><url>/ui_acme</url><handler><type>acme</type></handler></rule>
        <defaults/>
    </http_handlers>
</clickhouse>
XML

# This script alone controls the lifetime of the listener, not a timer: after
# `SYSTEM START LISTEN HTTP` the instance runs one query that reads a named pipe, so the listener
# stays bound for exactly as long as the script holds the write end of that pipe open. Closing it
# ends the query, the instance exits and the script waits for it, so no request can race a
# shutdown, however slow the machine is. The assigned port is likewise reported by the instance
# itself, on the log stream that `--send_logs_level` turns on, and read through a pipe rather than
# polled: the requests start exactly when the listener is up. No timing assumption anywhere.
rm -f "$LOGS" "$KEEP"
mkfifo "$LOGS" "$KEEP"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --send_logs_level=information --query "
    SYSTEM START LISTEN HTTP;
    SELECT count() FROM file('${KEEP}', LineAsString) FORMAT Null;
" > /dev/null 2> "$LOGS" &
listener=$!
# shellcheck disable=SC2064
trap "kill ${listener} 2>/dev/null; rm -f '$CONFIG' '$LOGS' '$KEEP' '$BODY' '$DEFAULT_BODY'" EXIT

port=""
exec 3< "$LOGS"
while read -r -u 3 log_line; do
    case "$log_line" in
        *'Listening for http://127.0.0.1:'*) port="${log_line##*:}"; break ;;
    esac
done
if ! [[ "$port" =~ ^[0-9]+$ ]]; then
    echo "FAIL: the instance reported no listening HTTP port"
    exit 1
fi

# The rest of the log stream is consumed so the instance never blocks on a full pipe.
cat <&3 > /dev/null &
exec 4> "$KEEP"

# The title of the page, or the first line of the body when it is not an HTML page: the `index`
# page answers a client that does not look like a browser with a plain `Ok.`.
payload() {
    local title
    title=$(grep -o -m1 '<title>[^<]*</title>' "$1" | sed -e 's|<title>||' -e 's|</title>||')
    if [ -n "$title" ]; then
        echo "$title"
    else
        head -n 1 "$1"
    fi
}

# Requests the configured url ($2) and the built-in path the same handler is registered on by
# `<defaults/>` ($3), and reports the status code, the decoded payload of the configured url and
# whether both urls returned the same page. Any further arguments are passed on to both requests.
check() {
    local description="$1" configured="$2" built_in="$3"
    shift 3
    local code same=0
    code=$($CLICKHOUSE_CURL -o "$BODY" -w '%{http_code}' "$@" "http://127.0.0.1:${port}${configured}")
    $CLICKHOUSE_CURL -o "$DEFAULT_BODY" "$@" "http://127.0.0.1:${port}${built_in}"
    cmp -s "$BODY" "$DEFAULT_BODY" && same=1
    echo "${description} ${code} $(payload "$BODY") same_as_default=${same}"
}

check '/ui_index' '/ui_index' '/'
check '/ui_index as a browser' '/ui_index' '/' --user-agent 'Mozilla/5.0'
check '/ui_jemalloc' '/ui_jemalloc' '/jemalloc'
check '/ui_schema' '/ui_schema' '/schema'
check '/ui_docs' '/ui_docs' '/docs'

echo "/ui_not_configured $($CLICKHOUSE_CURL -o /dev/null -w '%{http_code}' "http://127.0.0.1:${port}/ui_not_configured")"
echo "/ui_acme as HEAD $($CLICKHOUSE_CURL -o /dev/null -w '%{http_code}' --head "http://127.0.0.1:${port}/ui_acme")"

# Every request has been answered: release the keep-alive pipe and wait for the instance to go.
exec 4>&-
wait "$listener" 2>/dev/null
