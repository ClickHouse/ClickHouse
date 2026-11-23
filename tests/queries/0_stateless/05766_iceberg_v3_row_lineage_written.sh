#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro.

# An INSERT into a format version 3 Iceberg table writes row lineage: each snapshot gets `first-row-id` and
# `added-rows`, the table `next-row-id`, and each manifest list entry a `first_row_id` range. The ranges are
# contiguous from 0 to `next-row-id`. The Avro schemas carry the spec field ids: 520 for `first_row_id` in the
# manifest list, 142 for `data_file.first_row_id` in a manifest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./iceberg.lib
. "$CUR_DIR"/iceberg.lib

TABLE_PATH="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TABLE_PATH:?}"
trap 'rm -rf "${TABLE_PATH:?}"' EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t (id Int32, s String) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS iceberg_format_version = 3;
    INSERT INTO t VALUES (1, 'a'), (2, 'b');
    INSERT INTO t VALUES (3, 'c'), (4, 'd'), (5, 'e');
    SELECT count() FROM t;"

META=$(iceberg_latest_metadata "${TABLE_PATH}/metadata")
jq -r '(.snapshots | sort_by(."sequence-number")) as $snapshots
    | "format-version \(."format-version")",
      "next-row-id \(."next-row-id")",
      "first-row-id \([$snapshots[]."first-row-id"])",
      "added-rows \([$snapshots[]."added-rows"])",
      "summary added-records \([$snapshots[].summary."added-records" | tonumber])"' "${META}"

# Avro writer schema field id of the field $2 (of the record field $3, if given) in the Avro file $1.
field_id()
{
    python3 "${CUR_DIR}/helpers/avro_schema.py" "$1" \
        | jq --arg field "$2" --arg record "${3:-}" \
            '.fields | if $record != "" then (.[] | select(.name == $record) | .type.fields) end | .[] | select(.name == $field) | ."field-id"'
}

MANIFEST_LIST=$(jq -r '.snapshots | max_by(."sequence-number") | ."manifest-list"' "${META}")
echo "manifest list: first_row_id field id $(field_id "${MANIFEST_LIST}" first_row_id)"
# The ranges are contiguous from 0 and end at next-row-id.
${CLICKHOUSE_CLIENT} --query "SELECT first_row_id, added_rows_count FROM file('${MANIFEST_LIST}', Avro) ORDER BY first_row_id"
for manifest in $(${CLICKHOUSE_CLIENT} --query "SELECT manifest_path FROM file('${MANIFEST_LIST}', Avro) ORDER BY first_row_id"); do
    echo "manifest: data_file.first_row_id field id $(field_id "${manifest}" first_row_id data_file)"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
