#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Files with the same size and checksum are stored in a backup once, and `num_entries` counts the stored files.
# It must not depend on how the stored files are named: by the first file name (the default) or by the checksum.
# The backup names are unique because `system.backups` is shared by all tests.

DEFAULT="${CLICKHOUSE_TEST_UNIQUE_NAME}_default"
CHECKSUM="${CLICKHOUSE_TEST_UNIQUE_NAME}_checksum"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;

-- Two parts with the same data, so most of their files are identical.
SYSTEM STOP MERGES t;
INSERT INTO t SELECT number FROM numbers(10);
INSERT INTO t SELECT number FROM numbers(10);

BACKUP TABLE t TO Memory('${DEFAULT}') FORMAT Null;
BACKUP TABLE t TO Memory('${CHECKSUM}') SETTINGS data_file_name_generator = 'checksum' FORMAT Null;

SELECT
    maxIf(num_entries, name LIKE '%${DEFAULT}%') < maxIf(num_files, name LIKE '%${DEFAULT}%') AS duplicates_are_stored_once,
    maxIf(num_entries, name LIKE '%${CHECKSUM}%') = maxIf(num_entries, name LIKE '%${DEFAULT}%') AS num_entries_do_not_depend_on_generator
FROM system.backups
WHERE (name LIKE '%${DEFAULT}%' OR name LIKE '%${CHECKSUM}%') AND status = 'BACKUP_CREATED';

DROP TABLE t;
"
