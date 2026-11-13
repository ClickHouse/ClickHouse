#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A backup is user input: RESTORE checks the definition against the current settings, like CREATE does.

${CLICKHOUSE_CLIENT} -m -q "
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_suspicious_type (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_suspicious_type VALUES (1), (2);
BACKUP TABLE t_suspicious_type TO Disk('backups', '${CLICKHOUSE_DATABASE}_05629_type') FORMAT Null;
DROP TABLE t_suspicious_type SYNC;

SET allow_suspicious_codecs = 1;
CREATE TABLE t_suspicious_codec (x UInt64 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_suspicious_codec VALUES (3), (4);
BACKUP TABLE t_suspicious_codec TO Disk('backups', '${CLICKHOUSE_DATABASE}_05629_codec') FORMAT Null;
DROP TABLE t_suspicious_codec SYNC;
"

${CLICKHOUSE_CLIENT} -m -q "
SET allow_suspicious_low_cardinality_types = 0;
RESTORE TABLE t_suspicious_type FROM Disk('backups', '${CLICKHOUSE_DATABASE}_05629_type') FORMAT Null; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
RESTORE TABLE t_suspicious_type FROM Disk('backups', '${CLICKHOUSE_DATABASE}_05629_type') SETTINGS allow_suspicious_low_cardinality_types = 1 FORMAT Null;
SELECT x FROM t_suspicious_type ORDER BY x;
DROP TABLE t_suspicious_type;

SET allow_suspicious_codecs = 0;
RESTORE TABLE t_suspicious_codec FROM Disk('backups', '${CLICKHOUSE_DATABASE}_05629_codec') FORMAT Null; -- { serverError BAD_ARGUMENTS }
RESTORE TABLE t_suspicious_codec FROM Disk('backups', '${CLICKHOUSE_DATABASE}_05629_codec') SETTINGS allow_suspicious_codecs = 1 FORMAT Null;
SELECT x FROM t_suspicious_codec ORDER BY x;
DROP TABLE t_suspicious_codec;
"
