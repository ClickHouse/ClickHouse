#!/usr/bin/env bash

# Statistics are not declared per column anymore, they are collected according to the `auto_statistics_types` setting.
# The metadata stored by older versions can still contain the `STATISTICS` clause of a column, and must be attachable.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT --send_logs_level=error -q "
ATTACH TABLE t_attach_explicit_statistics UUID '$uuid'
(
    a UInt64 STATISTICS(tdigest, uniq),
    b String STATISTICS(countmin)
)
ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = 'basic'"

$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t_attach_explicit_statistics" | sed "s/$uuid/<uuid>/"

$CLICKHOUSE_CLIENT -n -q "
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' ORDER BY name;

INSERT INTO t_attach_explicit_statistics SELECT number, toString(number) FROM numbers(100) SETTINGS materialize_statistics_on_insert = 1;
SELECT column, statistics FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' AND active ORDER BY column;

-- \`DROP STATISTICS\` was stored in the mutation entries by older versions. It is an obsolete synonym of \`CLEAR STATISTICS\`.
ALTER TABLE t_attach_explicit_statistics DROP STATISTICS a SETTINGS mutations_sync = 2;
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics';
SELECT column, statistics FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' AND active ORDER BY column;

ALTER TABLE t_attach_explicit_statistics MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;
SELECT column, statistics FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_attach_explicit_statistics' AND active ORDER BY column;

DROP TABLE t_attach_explicit_statistics;
"
