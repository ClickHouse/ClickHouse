#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Iceberg needs Avro and S3.
# Tag no-parallel: uses a server-wide failpoint.

# `ALTER TABLE ... DROP PARTITION` on Iceberg fixes the set of files to drop on its first attempt and keeps it on retry.
# A file that a concurrent INSERT adds to the same partition after the discovery must survive the drop:
# the retry sees the new parent snapshot, but the new file is not in the set.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT="iceberg_drop_partition_pause_after_discovery"
ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ROOT:?}"
cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"
    rm -rf "${ROOT:?}"
}
trap cleanup EXIT

for storage in local s3; do
    echo "--- ${storage}"
    TABLE="t_${storage}"
    if [ "${storage}" = local ]; then
        ENGINE="IcebergLocal('${ROOT}/${TABLE}/', 'Parquet')"
    else
        ENGINE="IcebergS3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}/${TABLE}/')"
    fi

    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        CREATE TABLE ${TABLE} (a Int64, b String) ENGINE = ${ENGINE} PARTITION BY identity(a);
        INSERT INTO ${TABLE} VALUES (1, 'before-drop-1'), (1, 'before-drop-2'), (2, 'keep');
        SELECT count() FROM ${TABLE};
        SYSTEM ENABLE FAILPOINT ${FAILPOINT};"

    # The DROP pauses right after it discovers the files of partition a = 1.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1" &
    drop_pid=$!

    # The INSERT into the same partition commits a new snapshot while the DROP is paused. Then the DROP resumes:
    # its first commit fails on the moved parent snapshot, and the retry commits.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE;
        INSERT INTO ${TABLE} VALUES (1, 'inserted-during-drop');
        SYSTEM DISABLE FAILPOINT ${FAILPOINT};"
    wait "${drop_pid}"

    # The two files of a = 1 from before the DROP are gone; the concurrent row and partition a = 2 stay.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT a, b FROM ${TABLE} ORDER BY a, b;
        DROP TABLE ${TABLE};"
done
