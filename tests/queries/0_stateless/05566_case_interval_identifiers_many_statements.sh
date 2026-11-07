#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `case` and `interval` used as identifiers in every statement of a long script. Deciding whether they begin
# a `CASE` or `INTERVAL` expression must look only within the statement: the script is parsed statement by
# statement, and looking to the end of the script for each of them takes time quadratic in its length.
for _ in {1..2000}
do
    echo 'SELECT interval, case FROM t WHERE interval > 1;'
done | $CLICKHOUSE_FORMAT --multiquery --oneline --max_query_size 0 | sort | uniq -c | sed 's/^ *//'
