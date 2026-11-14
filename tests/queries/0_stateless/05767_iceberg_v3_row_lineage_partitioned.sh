#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and S3.

# An INSERT into a partitioned format version 3 Iceberg table writes one data file and one manifest per partition.
# The row id ranges of the manifests of a snapshot are contiguous from the snapshot `first-row-id` and cover its
# `added-rows`, and `_row_id` reads back as 0 .. `next-row-id` - 1.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./iceberg.lib
. "$CUR_DIR"/iceberg.lib

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ROOT:?}"
trap 'rm -rf "${ROOT:?}"' EXIT

for storage in local s3; do
    echo "--- ${storage}"
    TABLE="t_${storage}"
    TABLE_PATH="${ROOT}/${TABLE}"
    if [ "${storage}" = local ]; then
        ENGINE="IcebergLocal('${TABLE_PATH}/', 'Parquet')"
    else
        S3_PATH="${CLICKHOUSE_TEST_UNIQUE_NAME}/${TABLE}"
        ENGINE="IcebergS3(s3_conn, filename = '${S3_PATH}/')"
    fi

    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        CREATE TABLE ${TABLE} (part Int32, id Int32) ENGINE = ${ENGINE} ORDER BY id PARTITION BY part SETTINGS iceberg_format_version = 3;
        INSERT INTO ${TABLE} VALUES (1, 0), (1, 1), (1, 2), (2, 3), (2, 4);
        INSERT INTO ${TABLE} VALUES (1, 5), (2, 6), (2, 7), (2, 8);"

    # CREATE writes v1 and each INSERT one more version.
    if [ "${storage}" = local ]; then
        METADATA=$(cat "${TABLE_PATH}/metadata/v3.metadata.json")
    else
        METADATA=$(s3_cat "${S3_PATH}/metadata/v3.metadata.json")
    fi
    CURRENT=$(jq '."current-snapshot-id"' <<< "${METADATA}")
    if [ "${storage}" = local ]; then
        MANIFEST_LIST="file('${TABLE_PATH}/metadata/snap-${CURRENT}-*.avro', Avro)"
    else
        MANIFEST_LIST="s3(s3_conn, filename = '${S3_PATH}/metadata/snap-${CURRENT}-*.avro', format = Avro)"
    fi
    jq -r '(.snapshots | sort_by(."sequence-number")) as $snapshots
        | "format-version \(."format-version")",
          "next-row-id \(."next-row-id")",
          "first-row-id \([$snapshots[]."first-row-id"])",
          "added-rows \([$snapshots[]."added-rows"])"' <<< "${METADATA}"

    # Per snapshot in the current manifest list: whether it is the current one, its manifests, the first row id,
    # the rows they add, and where their ranges end. Contiguous ranges end at first row id + rows.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT added_snapshot_id = ${CURRENT}, count(), min(first_row_id) AS first, sum(added_rows_count),
            max(first_row_id + added_rows_count)
        FROM ${MANIFEST_LIST}
        GROUP BY added_snapshot_id
        ORDER BY first"

    ${CLICKHOUSE_CLIENT} --query "
        SELECT groupArray(_row_id) FROM (SELECT _row_id FROM ${TABLE} ORDER BY _row_id);
        DROP TABLE ${TABLE};"
done
