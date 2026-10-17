#!/usr/bin/env bash

# Columns with empty names are removed from the inferred schema, but the schema was cached before they were removed,
# so the next read of the same file got the schema with the column with an empty name from the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CLICKHOUSE_TMP/$CLICKHOUSE_DATABASE.csv"
printf '"",x\n1,2\n' > "$FILE"
# The schema cache is not used for a file modified in the same second as the schema was cached.
touch -d '2020-01-01 00:00:00' "$FILE"

$CLICKHOUSE_LOCAL --query "
    SELECT * FROM file('$FILE', CSVWithNames);
    SELECT * FROM file('$FILE', CSVWithNames);
    DESCRIBE file('$FILE', CSVWithNames);
"

rm -f "$FILE"
