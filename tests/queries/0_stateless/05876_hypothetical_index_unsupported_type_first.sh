#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# an unsupported hypothetical index type is rejected before its arguments are analyzed

user="u_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE events (id UInt32, val String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE secrets (secret String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO secrets VALUES ('hidden-value');
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT(id, val), SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.events TO ${user};
"

create_index()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "CREATE HYPOTHETICAL INDEX i ON ${CLICKHOUSE_DATABASE}.events (val) TYPE $1" 2>&1
}

echo "a transform that reads a table the user cannot read"
out=$(create_index "text(tokenizer = array, preprocessor = concat(val, toString((SELECT CAST(secret AS Enum8('na' = 1)) FROM ${CLICKHOUSE_DATABASE}.secrets LIMIT 1))))")
echo "$out" | grep -o '([A-Z_]*)' | tail -1
echo "$out" | grep -c 'hidden-value'

echo "a transform with a wrong number of arguments"
create_index "text(tokenizer = array, preprocessor = lower(val, val))" | grep -o '([A-Z_]*)' | tail -1

echo "a tokenizer that does not exist"
create_index "text(tokenizer = no_such_tokenizer)" | grep -o '([A-Z_]*)' | tail -1

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
