#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client prints the failed query next to its error, without the inline data of an INSERT: the
# data is outside the AST, so its secrets cannot be hidden. In the polyglot dialect the parser
# clears the inline-data pointers of the INSERT (they would reference its transient transpiled
# buffer), so the client has to recognize such an INSERT by `inline_data_owned_by_transpiled_query`.
# The INSERT below is parsed successfully and is rejected by the server before any data is decoded.

POLY="--enable_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"
QUERY="INSERT INTO definitely_missing_table VALUES ('private-row-sentinel')"

function check()
{
    local name=$1
    shift
    local output
    output=$("$@" 2>&1)
    echo "$name: $(grep -om1 'UNKNOWN_TABLE' <<< "$output"), $(grep -om1 'query: INSERT INTO' <<< "$output"), data printed: $(grep -c 'private-row-sentinel' <<< "$output")"
}

# The script mode (`--queries-file`) reports the error on a separate code path. The polyglot dialect
# accepts a single statement only, so the script is that one statement.
QUERIES_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
echo "$QUERY" > "$QUERIES_FILE"

# The native dialect is the control: it has always omitted the data.
check "native, single query" $CLICKHOUSE_CLIENT -q "$QUERY"
check "native, script" $CLICKHOUSE_CLIENT --queries-file "$QUERIES_FILE"

check "polyglot, single query" $CLICKHOUSE_CLIENT $POLY -q "$QUERY"
check "polyglot, script" $CLICKHOUSE_CLIENT $POLY --queries-file "$QUERIES_FILE"

rm -f "$QUERIES_FILE"
