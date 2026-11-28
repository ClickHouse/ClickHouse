#!/usr/bin/env bash
# Tags: shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The current database of the query has a dot in its name.
DB="${CLICKHOUSE_DATABASE}.dot"

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE \`$DB\`;
    USE \`$DB\`;
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    INSERT INTO t VALUES (2), (3);
    CREATE DICTIONARY d (x UInt64) PRIMARY KEY x SOURCE(CLICKHOUSE(DB '$DB' TABLE 't')) LIFETIME(0) LAYOUT(HASHED());
    SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM loop(t) LIMIT 2);
    SELECT sum(x) FROM (SELECT x FROM loop(dictionary('d')) LIMIT 2);
"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\`"
