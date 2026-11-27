#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

# SYSTEM RELOAD DICTIONARIES finishes when SYSTEM STOP RELOAD DICTIONARIES blocks one of its reloads while it waits,
# instead of rescheduling the blocked reload forever.

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
LIFETIME(0);

SELECT dictGet('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1));
SYSTEM ENABLE FAILPOINT ${FAILPOINT};
EOF

# The fail point pauses the first reload this command starts; the command then waits for it.
timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARIES" > /dev/null 2>&1 &
reload_pid=$!

$CLICKHOUSE_CLIENT <<EOF
SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE;
SYSTEM STOP RELOAD DICTIONARIES;
SYSTEM NOTIFY FAILPOINT ${FAILPOINT};
EOF

# Whether it throws does not matter here: SYSTEM RELOAD DICTIONARIES throws once reloads are stopped (see 03356).
# 124 is the exit code of `timeout` when the command hung.
reload_exit_code=0
wait "$reload_pid" || reload_exit_code=$?
if [ "$reload_exit_code" -eq 124 ]; then
    echo "SYSTEM RELOAD DICTIONARIES hung"
else
    echo "SYSTEM RELOAD DICTIONARIES finished"
fi

# The previous version stays available while reloads are stopped.
$CLICKHOUSE_CLIENT --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1))"
