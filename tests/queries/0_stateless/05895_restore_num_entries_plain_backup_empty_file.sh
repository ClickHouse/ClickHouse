#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RESTORE reports the same num_entries as the BACKUP it reads. A backup with deduplicate_files = 0
# stores every file, empty ones too: the element stream of the empty arrays below is a zero-byte file.

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE t (s Array(UInt64)) ENGINE = Log;
INSERT INTO t SELECT [] FROM numbers(10);
"

for layout in plain dedup; do
    if [ "$layout" = plain ]; then dedup=0; else dedup=1; fi
    name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${layout}"
    ${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$name')
        SETTINGS deduplicate_files = $dedup, id = '${name}_backup' FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_$layout
        FROM Disk('backups', '$name') SETTINGS id = '${name}_restore' FORMAT Null"
    # layout, RESTORE num_files = BACKUP num_files, RESTORE num_entries = BACKUP num_entries,
    # BACKUP stores every file (num_entries = num_files)
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '$layout', r.num_files = b.num_files, r.num_entries = b.num_entries, b.num_entries = b.num_files
        FROM system.backups AS b, system.backups AS r
        WHERE b.id = '${name}_backup' AND r.id = '${name}_restore'"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(length(s)) FROM ${CLICKHOUSE_DATABASE}.t_$layout"
done
