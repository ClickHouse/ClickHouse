#!/usr/bin/env bash

# An `ALTER` with a `DEFAULT` written inside a tuple element checks the privileges of the user before it
# finds out whether the table exists, so a user without privileges gets `ACCESS_DENIED` for both an existing
# and a missing table, with `MODIFY COLUMN` as well as with `ADD COLUMN`.
# See https://github.com/ClickHouse/ClickHouse/issues/2797.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05465_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    CREATE TABLE t_exists (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
"

for table in t_exists t_missing
do
    $CLICKHOUSE_CLIENT --user "$user" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.$table MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT \`c.a\`)" 2>&1 \
        | grep -o -m1 'ACCESS_DENIED\|BAD_ARGUMENTS'
    $CLICKHOUSE_CLIENT --user "$user" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.$table ADD COLUMN d Tuple(a UInt8, b UInt8 DEFAULT \`d.a\`)" 2>&1 \
        | grep -o -m1 'ACCESS_DENIED\|BAD_ARGUMENTS'
done

$CLICKHOUSE_CLIENT -m -q "
    DROP USER $user;
    DROP TABLE t_exists;
"
