#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel

# The declaration of a partial index is authorized as the user, but the stored definition is not checked again by an unrelated ALTER.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05461_alter_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
SET allow_experimental_partial_skip_indexes = 1;
CREATE TABLE stored (p UInt8, INDEX i p TYPE minmax WHERE p > 1) ENGINE = MergeTree ORDER BY tuple();
CREATE USER $user;
GRANT ALTER ADD COLUMN ON $CLICKHOUSE_DATABASE.stored TO $user;
"

$CLICKHOUSE_CLIENT --user "$user" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.stored ADD COLUMN z UInt8" && echo "altered"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.columns WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'stored' ORDER BY name"

$CLICKHOUSE_CLIENT -q "
DROP USER $user;
DROP TABLE stored;
"
