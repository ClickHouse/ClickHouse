#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the YAML document of a `YAMLRegExpTree` dictionary source is not a list of
# match rules, loading fails. The error message must not echo the parsed document
# back to the client: it is file content, and leaking it to the querying user is
# an information disclosure. Point the source at a file whose content is a scalar
# holding a recognizable marker and assert the marker never reaches the output.

mkdir -p "$USER_FILES_PATH/test_05493"
yaml="$USER_FILES_PATH/test_05493/test.yaml"
echo "SECRET_FILE_CONTENT_MARKER" > "$yaml"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.no_leak"

$CLICKHOUSE_CLIENT --query "
    CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.no_leak
    (regexp String, value String)
    PRIMARY KEY regexp
    SOURCE(YAMLRegExpTree(PATH '$yaml'))
    LAYOUT(regexp_tree) LIFETIME(0)"

# Loading fails with the fixed message and without echoing the file content.
output=$($CLICKHOUSE_CLIENT --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.no_leak', 'value', 'anything')" 2>&1)
echo "$output" | grep -a -q "SECRET_FILE_CONTENT_MARKER" && echo "LEAK" || echo "no leak"
echo "$output" | grep -a -q "must be a yaml list of match rules" && echo "expected error" || echo "MISSING EXPECTED ERROR"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.no_leak"
rm -rf "$USER_FILES_PATH/test_05493"
