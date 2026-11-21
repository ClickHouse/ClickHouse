#!/usr/bin/env bash

# An `ALTER` with a `DEFAULT` written inside a tuple element consults the table schema (to tell an
# independent column named `c.a` from a subcolumn of `c`) only after checking the privileges of the user,
# so a user without privileges gets `ACCESS_DENIED` regardless of which columns exist.
# See https://github.com/ClickHouse/ClickHouse/issues/2797.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05464_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    CREATE TABLE t_with_dotted (id UInt8, \`c.a\` UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
    CREATE TABLE t_without_dotted (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
"

for table in t_with_dotted t_without_dotted
do
    $CLICKHOUSE_CLIENT --user "$user" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.$table MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT \`c.a\`)" 2>&1 \
        | grep -o -m1 'ACCESS_DENIED\|BAD_ARGUMENTS'
done

$CLICKHOUSE_CLIENT -m -q "
    DROP USER $user;
    DROP TABLE t_with_dotted;
    DROP TABLE t_without_dotted;
"
