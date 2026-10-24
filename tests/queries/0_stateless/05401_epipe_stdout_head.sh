#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the reader of the output goes away, as `head` does, the client must stop quietly:
# no `Broken pipe` exception on stderr and a zero exit code.
# stderr is captured separately: with `2>&1 | head` the message would be cut off by `head` too.

stderr_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"

function check()
{
    local name=$1
    shift
    "$@" 2>"$stderr_file" | head -1
    echo "$name: exit code ${PIPESTATUS[0]}"
    cat "$stderr_file"
}

check client $CLICKHOUSE_CLIENT -q "SELECT * FROM numbers(1000000)"
check local $CLICKHOUSE_LOCAL -q "SELECT * FROM numbers(1000000)"
check "local, without parallel formatting" $CLICKHOUSE_LOCAL --output_format_parallel_formatting 0 -q "SELECT * FROM numbers(1000000)"

# The rest of the queries is not run, even with `--ignore-error`.
check "client, ignore error" $CLICKHOUSE_CLIENT --ignore-error -q "SELECT * FROM numbers(1000000); SELECT throwIf(1)"
check "local, ignore error" $CLICKHOUSE_LOCAL --ignore-error -q "SELECT * FROM numbers(1000000); SELECT throwIf(1)"

# INTO OUTFILE ... AND STDOUT uses a ForkWriteBuffer with a separate stdout writer.
# EPIPE on that writer must also be handled quietly.
out_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.outfile.tsv"
next_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.next.tsv"

rm -f "$out_file" "$next_file"
check "local, into outfile and stdout" $CLICKHOUSE_LOCAL -q "SELECT * FROM numbers(1000000) INTO OUTFILE '$out_file' AND STDOUT"

rm -f "$out_file" "$next_file"
check "local, into outfile and stdout, ignore error" $CLICKHOUSE_LOCAL --ignore-error -q "SELECT * FROM numbers(1000000) INTO OUTFILE '$out_file' AND STDOUT; SELECT 42 INTO OUTFILE '$next_file'"
if [ -f "$next_file" ]; then
    echo "FAIL: next query ran after broken stdout"
else
    echo "OK: next query was skipped"
fi

rm -f "$stderr_file" "$out_file"
