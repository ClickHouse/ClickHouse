#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro.

# A snapshot summary without the spec-optional metrics `added-files-size` and `changed-partition-count`
# must be readable: `OPTIMIZE TABLE` must not fail with `Can not convert empty value`, and
# `system.iceberg_history` must not drop the rows. https://github.com/ClickHouse/ClickHouse/issues/89037

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./iceberg.lib
. "$CUR_DIR"/iceberg.lib

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ROOT:?}"
trap 'rm -rf "${ROOT:?}"' EXIT

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

for fv in 1 2; do
    echo "--- format version ${fv}"
    TABLE="t_fv${fv}"
    TABLE_PATH="${ROOT}/${TABLE}/"

    # Two `append` snapshots: `getHistory` parses every summary, and an `append` passes the checks after it.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        CREATE TABLE ${TABLE} (x Int) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version = ${fv};
        INSERT INTO ${TABLE} VALUES (1);
        INSERT INTO ${TABLE} VALUES (2);"

    # Strip both metrics from every snapshot summary.
    # `added-data-files` stays: an append with no added files is rejected before the summary matters.
    iceberg_edit_metadata "${TABLE_PATH}metadata" '.snapshots[].summary |= del(."added-files-size", ."changed-partition-count")' > /dev/null

    # `IcebergMetadata::optimize` reads the history before the compaction, which is v2-only. The cloud
    # build gates `OPTIMIZE` by a member flag instead and may reject it with its own error. An unexpected
    # error, `Can not convert empty value` on any build, is printed in full and breaks the reference.
    if ! out=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1); then
        if grep -q 'Can not convert empty value' <<< "${out}" \
            || { [ "${IS_CLOUD}" != 1 ] && ! grep -q 'Compaction is supported only for format_version 2' <<< "${out}"; }; then
            echo "${out}"
        fi
    fi

    # `StorageSystemIcebergHistory` swallows a `getHistory` exception, so the row count detects it.
    # The 0 values show the stripped version was read: an unstripped summary has a non-zero size.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(summary['added-files-size'] = '0'), countIf(summary['changed-partition-count'] = '0')
        FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}';
        SELECT x FROM ${TABLE} ORDER BY x;
        DROP TABLE ${TABLE} SYNC;"
done
