#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A string literal that ends in an incomplete UTF-8 sequence right before `)` or at the end of the query.
for query in \
    "SELECT length(x'E2')" \
    "SELECT length(x'F0')" \
    "SELECT x'F0'" \
    "SELECT * FROM format(Native, '\x01\x01\x01x\x0CArray(UInt8)\x01\x00\xBD\xEF\xBF\xBD\xEF\xBF\xBD\xEF')"
do
    highlighted=$(TERM=xterm ${CLICKHOUSE_FORMAT} --hilite --query "$query" | sed 's/\x1b\[[0-9;]*m//g' | od -An -tx1)
    plain=$(${CLICKHOUSE_FORMAT} --query "$query" | od -An -tx1)
    [ "$highlighted" = "$plain" ] && echo "ok" || echo "mismatch: $query"
done

# The closing bracket keeps its own color.
TERM=xterm ${CLICKHOUSE_FORMAT} --hilite --query "SELECT length(x'E2')" | cat -v
