#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

# A periodic reload of a loaded dictionary that is already queued when SYSTEM STOP RELOAD DICTIONARIES returns
# does not read the source: the change stays invisible until SYSTEM START RELOAD DICTIONARIES.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

FAILPOINT=external_loader_pause_before_loading

# Reloads are stopped server-wide: never leave them stopped, even when the test fails.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"; $CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"' EXIT

$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE ${CLICKHOUSE_DATABASE}.src (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.src VALUES (1, 100);

CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict (id UInt64, val Int64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(FLAT())
LIFETIME(MIN 1 MAX 1);

SELECT dictGet('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1));
SYSTEM ENABLE FAILPOINT ${FAILPOINT};
SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE;
TRUNCATE TABLE ${CLICKHOUSE_DATABASE}.src;
INSERT INTO ${CLICKHOUSE_DATABASE}.src VALUES (1, 200);
SYSTEM STOP RELOAD DICTIONARIES;
SYSTEM NOTIFY FAILPOINT ${FAILPOINT};
EOF

function dict_status()
{
    $CLICKHOUSE_CLIENT --query "SELECT status FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'dict'"
}

# The released periodic reload ends, whether it loaded or was blocked.
for _ in {1..300}; do
    [ "$(dict_status)" = "LOADED" ] && break
    sleep 0.1
done
echo "while stopped: $(dict_status)"
$CLICKHOUSE_CLIENT --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1))"

# The blocked reload runs again on START.
$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"
for _ in {1..300}; do
    val=$($CLICKHOUSE_CLIENT --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1))")
    [ "$val" = "200" ] && break
    sleep 0.1
done
echo "after START: $val"
