#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RESTORE loads a table as it was backed up: a type accepted when the table was created is not checked again
# against the current settings. A new definition is still checked.

BACKUP_NAME="${CLICKHOUSE_DATABASE}_05629"

${CLICKHOUSE_CLIENT} -m -q "
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_suspicious (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_suspicious VALUES (1), (2);
BACKUP TABLE t_suspicious TO Disk('backups', '${BACKUP_NAME}') FORMAT Null;
DROP TABLE t_suspicious SYNC;
"

${CLICKHOUSE_CLIENT} -m -q "
SET allow_suspicious_low_cardinality_types = 0;
CREATE TABLE t_suspicious (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
RESTORE TABLE t_suspicious FROM Disk('backups', '${BACKUP_NAME}') FORMAT Null;
SELECT x FROM t_suspicious ORDER BY x;
DROP TABLE t_suspicious;
"
