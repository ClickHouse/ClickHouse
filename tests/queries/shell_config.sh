#!/usr/bin/env bash
# shellcheck disable=SC2120

# If ClickHouse was built with coverage - dump the coverage information at exit
# (in other cases this environment variable has no effect)
export CLICKHOUSE_WRITE_COVERAGE="coverage"

export CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:="test"}
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL:="warning"}

# Unique zookeeper path (based on test name and current database) to avoid overlaps
export CLICKHOUSE_TEST_PATH="${BASH_SOURCE[1]}"
CLICKHOUSE_TEST_NAME="$(basename "$CLICKHOUSE_TEST_PATH")"
CLICKHOUSE_TEST_NAME="${CLICKHOUSE_TEST_NAME%%.*}"
export CLICKHOUSE_TEST_NAME
export CLICKHOUSE_TEST_ZOOKEEPER_PREFIX="${CLICKHOUSE_TEST_NAME}_${CLICKHOUSE_DATABASE}"
export CLICKHOUSE_TEST_UNIQUE_NAME="${CLICKHOUSE_TEST_NAME}_${CLICKHOUSE_DATABASE}"

[ -n "${CLICKHOUSE_CONFIG_CLIENT:-}" ] && CLICKHOUSE_CLIENT_OPT0+=" --config-file=${CLICKHOUSE_CONFIG_CLIENT} "
[ -n "${CLICKHOUSE_HOST:-}" ] && CLICKHOUSE_CLIENT_OPT0+=" --host=${CLICKHOUSE_HOST} "
[ -n "${CLICKHOUSE_PORT_TCP:-}" ] && CLICKHOUSE_CLIENT_OPT0+=" --port=${CLICKHOUSE_PORT_TCP} "
[ -n "${CLICKHOUSE_PORT_TCP:-}" ] && CLICKHOUSE_BENCHMARK_OPT0+=" --port=${CLICKHOUSE_PORT_TCP} "
[ -n "${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL:-}" ] && CLICKHOUSE_CLIENT_OPT0+=" --send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL} "
[ -n "${CLICKHOUSE_DATABASE:-}" ] && CLICKHOUSE_CLIENT_OPT0+=" --database=${CLICKHOUSE_DATABASE} "
[ -n "${CLICKHOUSE_LOG_COMMENT:-}" ] && CLICKHOUSE_CLIENT_OPT0+=" --log_comment $(printf '%q' ${CLICKHOUSE_LOG_COMMENT}) "
[ -n "${CLICKHOUSE_DATABASE:-}" ] && CLICKHOUSE_BENCHMARK_OPT0+=" --database=${CLICKHOUSE_DATABASE} "
[ -n "${CLICKHOUSE_LOG_COMMENT:-}" ] && CLICKHOUSE_BENCHMARK_OPT0+=" --log_comment $(printf '%q' ${CLICKHOUSE_LOG_COMMENT}) "

export CLICKHOUSE_BINARY=${CLICKHOUSE_BINARY:="$(command -v clickhouse)"}
# client
[ -x "$CLICKHOUSE_BINARY-client" ] && CLICKHOUSE_CLIENT_BINARY=${CLICKHOUSE_CLIENT_BINARY:=$CLICKHOUSE_BINARY-client}
[ -x "$CLICKHOUSE_BINARY" ] && CLICKHOUSE_CLIENT_BINARY=${CLICKHOUSE_CLIENT_BINARY:=$CLICKHOUSE_BINARY client}
export CLICKHOUSE_CLIENT_BINARY=${CLICKHOUSE_CLIENT_BINARY:=$CLICKHOUSE_BINARY-client}
export CLICKHOUSE_CLIENT_OPT="${CLICKHOUSE_CLIENT_OPT0:-} ${CLICKHOUSE_CLIENT_OPT:-}"
export CLICKHOUSE_CLIENT_EXPECT_OPT="${CLICKHOUSE_CLIENT_OPT} --disable_suggestion --no-warnings --enable-progress-table-toggle 0 --progress no --output-format-pretty-color 0 --highlight 0"
export CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT:="$CLICKHOUSE_CLIENT_BINARY ${CLICKHOUSE_CLIENT_OPT:-}"}
# local
[ -x "${CLICKHOUSE_BINARY}-local" ] && CLICKHOUSE_LOCAL=${CLICKHOUSE_LOCAL:="${CLICKHOUSE_BINARY}-local"}
[ -x "${CLICKHOUSE_BINARY}" ] && CLICKHOUSE_LOCAL=${CLICKHOUSE_LOCAL:="${CLICKHOUSE_BINARY} local"}
export CLICKHOUSE_LOCAL=${CLICKHOUSE_LOCAL:="${CLICKHOUSE_BINARY}-local"}
# server
[ -x "${CLICKHOUSE_BINARY}-server" ] && CLICKHOUSE_SERVER_BINARY=${CLICKHOUSE_SERVER_BINARY:="${CLICKHOUSE_BINARY}-server"}
[ -x "${CLICKHOUSE_BINARY}" ] && CLICKHOUSE_SERVER_BINARY=${CLICKHOUSE_SERVER_BINARY:="${CLICKHOUSE_BINARY} server"}
export CLICKHOUSE_SERVER_BINARY=${CLICKHOUSE_SERVER_BINARY:="${CLICKHOUSE_BINARY}-server"}
# benchmark
[ -x "${CLICKHOUSE_BINARY}-benchmark" ] && CLICKHOUSE_BENCHMARK_BINARY=${CLICKHOUSE_BENCHMARK_BINARY:="${CLICKHOUSE_BINARY}-benchmark"}
[ -x "${CLICKHOUSE_BINARY}" ] && CLICKHOUSE_BENCHMARK_BINARY=${CLICKHOUSE_BENCHMARK_BINARY:="${CLICKHOUSE_BINARY} benchmark"}
export CLICKHOUSE_BENCHMARK_BINARY="${CLICKHOUSE_BENCHMARK_BINARY:=${CLICKHOUSE_BINARY}-benchmark}"
export CLICKHOUSE_BENCHMARK_OPT="${CLICKHOUSE_BENCHMARK_OPT0:-} ${CLICKHOUSE_BENCHMARK_OPT:-}"
export CLICKHOUSE_BENCHMARK=${CLICKHOUSE_BENCHMARK:="$CLICKHOUSE_BENCHMARK_BINARY ${CLICKHOUSE_BENCHMARK_OPT:-}"}
# others
export CLICKHOUSE_OBFUSCATOR=${CLICKHOUSE_OBFUSCATOR:="${CLICKHOUSE_BINARY}-obfuscator"}
export CLICKHOUSE_COMPRESSOR=${CLICKHOUSE_COMPRESSOR:="${CLICKHOUSE_BINARY}-compressor"}

export CLICKHOUSE_CONFIG_DIR=${CLICKHOUSE_CONFIG_DIR:="/etc/clickhouse-server"}
export CLICKHOUSE_CONFIG=${CLICKHOUSE_CONFIG:="/etc/clickhouse-server/config.xml"}
export CLICKHOUSE_CONFIG_CLIENT=${CLICKHOUSE_CONFIG_CLIENT:="/etc/clickhouse-client/config.xml"}

export CLICKHOUSE_USER_FILES=${CLICKHOUSE_USER_FILES:="/var/lib/clickhouse/user_files"}
export CLICKHOUSE_USER_FILES_UNIQUE=${CLICKHOUSE_USER_FILES_UNIQUE:="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"}
# synonym
export USER_FILES_PATH=$CLICKHOUSE_USER_FILES

export CLICKHOUSE_SCHEMA_FILES=${CLICKHOUSE_SCHEMA_FILES:="/var/lib/clickhouse/format_schemas"}
export CLICKHOUSE_DISKS_FILES=${CLICKHOUSE_DISKS_FILES:="/var/lib/clickhouse/disks"}

[ -x "${CLICKHOUSE_BINARY}-extract-from-config" ] && CLICKHOUSE_EXTRACT_CONFIG=${CLICKHOUSE_EXTRACT_CONFIG:="$CLICKHOUSE_BINARY-extract-from-config --config=$CLICKHOUSE_CONFIG"}
[ -x "${CLICKHOUSE_BINARY}" ] && CLICKHOUSE_EXTRACT_CONFIG=${CLICKHOUSE_EXTRACT_CONFIG:="$CLICKHOUSE_BINARY extract-from-config --config=$CLICKHOUSE_CONFIG"}
export CLICKHOUSE_EXTRACT_CONFIG=${CLICKHOUSE_EXTRACT_CONFIG:="$CLICKHOUSE_BINARY-extract-from-config --config=$CLICKHOUSE_CONFIG"}

[ -x "${CLICKHOUSE_BINARY}-format" ] && CLICKHOUSE_FORMAT=${CLICKHOUSE_FORMAT:="$CLICKHOUSE_BINARY-format"}
[ -x "${CLICKHOUSE_BINARY}" ] && CLICKHOUSE_FORMAT=${CLICKHOUSE_FORMAT:="$CLICKHOUSE_BINARY format"}
export CLICKHOUSE_FORMAT=${CLICKHOUSE_FORMAT:="$CLICKHOUSE_BINARY-format"}

export CLICKHOUSE_CONFIG_GREP=${CLICKHOUSE_CONFIG_GREP:="/etc/clickhouse-server/preprocessed/config.xml"}

export CLICKHOUSE_HOST=${CLICKHOUSE_HOST:="localhost"}
export CLICKHOUSE_PORT_TCP=${CLICKHOUSE_PORT_TCP:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=tcp_port 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_TCP=${CLICKHOUSE_PORT_TCP:="9000"}
export CLICKHOUSE_PORT_TCP_SECURE=${CLICKHOUSE_PORT_TCP_SECURE:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=tcp_port_secure 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_TCP_SECURE=${CLICKHOUSE_PORT_TCP_SECURE:="9440"}
export CLICKHOUSE_PORT_TCP_WITH_PROXY=${CLICKHOUSE_PORT_TCP_WITH_PROXY:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=tcp_with_proxy_port 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_TCP_WITH_PROXY=${CLICKHOUSE_PORT_TCP_WITH_PROXY:="9010"}
export CLICKHOUSE_PORT_HTTP=${CLICKHOUSE_PORT_HTTP:=$(${CLICKHOUSE_EXTRACT_CONFIG} --key=http_port 2>/dev/null)}
export CLICKHOUSE_PORT_HTTP=${CLICKHOUSE_PORT_HTTP:="8123"}
export CLICKHOUSE_PORT_PROMTHEUS_PORT=${CLICKHOUSE_PORT_PROMTHEUS_PORT:=$(${CLICKHOUSE_EXTRACT_CONFIG} --key=prometheus.port 2>/dev/null)}
export CLICKHOUSE_PORT_PROMTHEUS_PORT=${CLICKHOUSE_PORT_PROMTHEUS_PORT:="9988"}
export CLICKHOUSE_PORT_HTTPS=${CLICKHOUSE_PORT_HTTPS:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=https_port 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_HTTPS=${CLICKHOUSE_PORT_HTTPS:="8443"}
export CLICKHOUSE_PORT_HTTP_PROTO=${CLICKHOUSE_PORT_HTTP_PROTO:="http"}
export CLICKHOUSE_PORT_MYSQL=${CLICKHOUSE_PORT_MYSQL:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=mysql_port 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_MYSQL=${CLICKHOUSE_PORT_MYSQL:="9004"}
export CLICKHOUSE_PORT_POSTGRESQL=${CLICKHOUSE_PORT_POSTGRESQL:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=postgresql_port 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_POSTGRESQL=${CLICKHOUSE_PORT_POSTGRESQL:="9005"}
export CLICKHOUSE_PORT_KEEPER=${CLICKHOUSE_PORT_KEEPER:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=keeper_server.tcp_port 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_KEEPER=${CLICKHOUSE_PORT_KEEPER:="9181"}

export CLICKHOUSE_KEEPER_IDENTITY=${CLICKHOUSE_KEEPER_IDENTITY:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=zookeeper.identity 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_KEEPER_IDENTITY=${CLICKHOUSE_KEEPER_IDENTITY:=""}

# keeper-client

KEEPER_CLIENT_DEFAULT_ARGS=" --port $CLICKHOUSE_PORT_KEEPER"

if [ -n "$CLICKHOUSE_KEEPER_IDENTITY" ] && [ "$CLICKHOUSE_KEEPER_IDENTITY" != "" ]
then
    KEEPER_CLIENT_DEFAULT_ARGS="$KEEPER_CLIENT_DEFAULT_ARGS --identity $CLICKHOUSE_KEEPER_IDENTITY"
fi

[ -x "${CLICKHOUSE_BINARY}-keeper-client" ] && CLICKHOUSE_KEEPER_CLIENT=${CLICKHOUSE_KEEPER_CLIENT:="${CLICKHOUSE_BINARY}-keeper-client $KEEPER_CLIENT_DEFAULT_ARGS"}
[ -x "${CLICKHOUSE_BINARY}" ] && CLICKHOUSE_KEEPER_CLIENT=${CLICKHOUSE_KEEPER_CLIENT:="${CLICKHOUSE_BINARY} keeper-client $KEEPER_CLIENT_DEFAULT_ARGS"}
export CLICKHOUSE_KEEPER_CLIENT=${CLICKHOUSE_KEEPER_CLIENT:="${CLICKHOUSE_BINARY}-keeper-client $KEEPER_CLIENT_DEFAULT_ARGS"}

export CLICKHOUSE_CLIENT_SECURE=${CLICKHOUSE_CLIENT_SECURE:=$(echo "${CLICKHOUSE_CLIENT}" | sed 's/--secure //' | sed 's/'"--port=${CLICKHOUSE_PORT_TCP}"'//g; s/$/'"--secure --accept-invalid-certificate --port=${CLICKHOUSE_PORT_TCP_SECURE}"'/g')}

# Add database and log comment to url params
if [ -n "${CLICKHOUSE_URL_PARAMS:-}" ]
then
  export CLICKHOUSE_URL_PARAMS="${CLICKHOUSE_URL_PARAMS}&database=${CLICKHOUSE_DATABASE}"
else
  export CLICKHOUSE_URL_PARAMS="database=${CLICKHOUSE_DATABASE}"
fi
# Note: missing url encoding of the log comment.
[ -n "${CLICKHOUSE_LOG_COMMENT:-}" ] && export CLICKHOUSE_URL_PARAMS="${CLICKHOUSE_URL_PARAMS}&log_comment=${CLICKHOUSE_LOG_COMMENT}"

export CLICKHOUSE_URL=${CLICKHOUSE_URL:="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"}
export CLICKHOUSE_URL_HTTPS=${CLICKHOUSE_URL_HTTPS:="https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTPS}/"}

# Add url params to url
if [ -n "${CLICKHOUSE_URL_PARAMS:-}" ]
then
  export CLICKHOUSE_URL="${CLICKHOUSE_URL}?${CLICKHOUSE_URL_PARAMS}"
  export CLICKHOUSE_URL_HTTPS="${CLICKHOUSE_URL_HTTPS}?${CLICKHOUSE_URL_PARAMS}"
fi

export CLICKHOUSE_URL_PROMETHEUS=${CLICKHOUSE_URL_PROMETHEUS:="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_PROMTHEUS_PORT}/metrics"}

export CLICKHOUSE_PORT_INTERSERVER=${CLICKHOUSE_PORT_INTERSERVER:=$(${CLICKHOUSE_EXTRACT_CONFIG} --try --key=interserver_http_port 2>/dev/null)} 2>/dev/null
export CLICKHOUSE_PORT_INTERSERVER=${CLICKHOUSE_PORT_INTERSERVER:="9009"}
export CLICKHOUSE_URL_INTERSERVER=${CLICKHOUSE_URL_INTERSERVER:="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_INTERSERVER}/"}

export CLICKHOUSE_CURL_COMMAND=${CLICKHOUSE_CURL_COMMAND:="curl"}
# The queries in CI are prone to sudden delays, and we often don't check for curl
# errors, so it makes sense to set a relatively generous timeout.
export CLICKHOUSE_CURL_TIMEOUT=${CLICKHOUSE_CURL_TIMEOUT:="120"}
export CLICKHOUSE_CURL=${CLICKHOUSE_CURL:="${CLICKHOUSE_CURL_COMMAND} -q -s --max-time ${CLICKHOUSE_CURL_TIMEOUT}"}
export CLICKHOUSE_TMP=${CLICKHOUSE_TMP:="."}
mkdir -p ${CLICKHOUSE_TMP}

export MYSQL_CLIENT_BINARY=${MYSQL_CLIENT_BINARY:="mysql"}
export MYSQL_CLIENT_CLICKHOUSE_USER=${MYSQL_CLIENT_CLICKHOUSE_USER:="default"}
# Avoids "Can't connect to local MySQL server through socket '/var/run/mysqld/mysqld.sock'" when connecting to localhost
[ -n "${CLICKHOUSE_HOST:-}" ] && MYSQL_CLIENT_OPT0+=" --protocol tcp "
[ -n "${CLICKHOUSE_HOST:-}" ] && MYSQL_CLIENT_OPT0+=" --host ${CLICKHOUSE_HOST} "
[ -n "${CLICKHOUSE_PORT_MYSQL:-}" ] && MYSQL_CLIENT_OPT0+=" --port ${CLICKHOUSE_PORT_MYSQL} "
[ -n "${CLICKHOUSE_DATABASE:-}" ] && MYSQL_CLIENT_OPT0+=" --database ${CLICKHOUSE_DATABASE} "
MYSQL_CLIENT_OPT0+=" --user ${MYSQL_CLIENT_CLICKHOUSE_USER} --no-auto-rehash "
export MYSQL_CLIENT_OPT="${MYSQL_CLIENT_OPT0:-} ${MYSQL_CLIENT_OPT:-}"
export MYSQL_CLIENT=${MYSQL_CLIENT:="$MYSQL_CLIENT_BINARY ${MYSQL_CLIENT_OPT:-}"}

export PROTOC_BINARY=${PROTOC_BINARY:="protoc"}

function clickhouse_client_removed_host_parameter()
{
    # removing only `--host=value` and `--host value` (removing '-hvalue' feels to dangerous) with python regex.
    # bash regex magic is arcane, but version dependant and weak; sed or awk are not really portable.
    $(echo "$CLICKHOUSE_CLIENT"  | python3 -c "import sys, re; print(re.sub(r'--host(\s+|=)[^\s]+', '', sys.stdin.read()))") "$@"
}

function wait_for_queries_to_finish()
{
    local max_tries="${1:-20}"
    # Wait for all queries to finish (query may still be running if a thread is killed by timeout)
    local num_tries=0
    while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE current_database=currentDatabase() AND query NOT LIKE '%system.processes%'") -ne 0 ]]; do
        sleep 0.5;
        num_tries=$((num_tries+1))
        if [ $num_tries -eq $max_tries ]; then
            $CLICKHOUSE_CLIENT -q "SELECT * FROM system.processes WHERE current_database=currentDatabase() AND query NOT LIKE '%system.processes%' FORMAT Vertical"
            break
        fi
    done
}

function random_str()
{
    local n=$1 && shift
    tr -cd '[:lower:]' < /dev/urandom | head -c"$n"
}

function query_with_retry()
{
    local query="$1" && shift

    local retry=0
    until [ $retry -ge 5 ]
    do
        local result
        result="$($CLICKHOUSE_CLIENT "$@" --query="$query" 2>&1)"
        if [ "$?" == 0 ]; then
            echo -n "$result"
            return
        else
            retry=$((retry + 1))
            sleep 3
        fi
    done
    echo "Query '$query' failed with '$result'"
}

function run_with_error()
{
    local cmd="$1"; shift

    local stdout_tmp=""
    stdout_tmp=$(mktemp -p ${CLICKHOUSE_TMP})
    local stderr_tmp=""
    stderr_tmp=$(mktemp -p ${CLICKHOUSE_TMP})

    local retval=0
    $cmd "$@" 1>${stdout_tmp} 2>${stderr_tmp} || retval="$?"

    echo "${retval}" "${stdout_tmp}" "${stderr_tmp}"

    return 0
}

# BASH_XTRACEFD is supported only since 4.1
if [[ -v CLICKHOUSE_BASH_TRACING_FILE ]] && [[ ${BASH_VERSINFO[0]} -gt 4 || (${BASH_VERSINFO[0]} -eq 4 && ${BASH_VERSINFO[1]} -ge 1) ]]; then
    exec 3>"$CLICKHOUSE_BASH_TRACING_FILE"
    # It will be also nice to have stderr in the tracing output, but:
    # - exec 2>&3
    #
    #   This will not preserve it in the stderr, and even though explicit
    #   stderr handling in tests will work, the check for non-empty stderr will
    #   not work at least
    #
    # - exec 2> >(stdbuf -o0 -e0 -i0 tee -a "$CLICKHOUSE_BASH_TRACING_FILE" >&2)
    #
    #   The problem with duplicating stderr with tee is bufferization, even
    #   with "tee -a" (opens with O_APPEND) and stdbuf I still got stderr after tracing
    #
    #   I've also tried unbuffer but it does not work
    #
    # But anyway it is useful even without stderr!
    #
    # Note, that we can redirect stderr into separate file, this should work,
    # but we will have to add a code to handle this in clickhouse-test wrapper,
    # but let's keep things simple for now.
    BASH_XTRACEFD=3
    export PS4='+ [\D{%Y-%m-%d %H:%M:%S}] [:${LINENO}] '
    set -x
fi
