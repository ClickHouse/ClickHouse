#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The intermediate result of a query whose SELECT list consists of window functions only has rows
# and no columns. With `--stage` the client is the consumer of that result, so the rows have to
# reach the output format: they used to be dropped as an empty block, both in `clickhouse-local`
# and in `clickhouse-client`, and the query printed nothing.

QUERY="SELECT count() OVER () FROM numbers(3) WHERE number > 0"

for client in "$CLICKHOUSE_LOCAL" "$CLICKHOUSE_CLIENT"
do
    echo "complete"
    $client --query "$QUERY"

    echo "with_mergeable_state, JSONCompactEachRow"
    $client --stage with_mergeable_state --format JSONCompactEachRow --query "$QUERY"

    echo "with_mergeable_state, JSONEachRow"
    $client --stage with_mergeable_state --format JSONEachRow --query "$QUERY"

    echo "with_mergeable_state, no rows"
    $client --stage with_mergeable_state --format JSONCompactEachRow --query "$QUERY AND number > 5"

    # A `Native` file is written at revision 0, which cannot represent rows without columns:
    # the query fails instead of silently writing nothing.
    echo "with_mergeable_state, Native"
    $client --stage with_mergeable_state --format Native --query "$QUERY" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED" | head -n1

    # The `Buffers` format takes the number of rows of a block from its columns, so it refuses such rows too.
    echo "with_mergeable_state, Buffers"
    $client --stage with_mergeable_state --format Buffers --query "$QUERY" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED" | head -n1
done
