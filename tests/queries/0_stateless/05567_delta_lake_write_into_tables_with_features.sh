#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# INSERT into Delta Lake tables written by Spark with table features (copies of corpus tables, see
# `05560_delta_lake_corpus`): appending to tables with deletion vectors, v2 checkpoints, and the change data feed
# works; with row tracking and in-commit timestamps the commit must carry `baseRowId`, `defaultRowCommitVersion`,
# `inCommitTimestamp`, and the new row id high water mark; tables with features that the writer does not support
# are rejected and stay unchanged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_USER_FILES_UNIQUE}_writes"
rm -rf "$BASE"
mkdir -p "$BASE"
for table in deletion_vectors v2_checkpoint_parquet change_data_feed_deletion_vectors row_tracking_in_commit_timestamps \
    type_widening liquid_clustering iceberg_compat_v2 column_mapping_id writer_features
do
    cp -r "$CUR_DIR/data_lake_corpus/delta/$table" "$BASE/$table"
done

SETTINGS="--allow_delta_lake_writes 1 --session_timezone UTC"

insert()
{
    local table=$1 values=$2
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $SETTINGS --query "INSERT INTO FUNCTION deltaLakeLocal('$BASE/$table') VALUES $values" 2>&1 \
        | grep -o -m1 -E '\(([A-Z_]+)\)$' || echo "inserted"
}

for table in deletion_vectors v2_checkpoint_parquet change_data_feed_deletion_vectors
do
    echo "-- $table: $(insert $table "(1000, 'new', 1)")"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), countIf(s = 'new') FROM deltaLakeLocal('$BASE/$table')"
done

echo '-- the change data feed of the appended rows'
${CLICKHOUSE_CLIENT} --query "
    SELECT _change_type, id, s FROM deltaLakeLocal('$BASE/change_data_feed_deletion_vectors')
    ORDER BY ALL SETTINGS delta_lake_snapshot_start_version = 5"

LOG="$BASE/row_tracking_in_commit_timestamps/_delta_log"
PREVIOUS=$(find "$LOG" -maxdepth 1 -name '*.json' | sort | tail -n 1)
echo "-- row tracking and in-commit timestamps: $(insert row_tracking_in_commit_timestamps "(100, 'new', 0), (101, 'new', 1)")"
LAST=$(find "$LOG" -maxdepth 1 -name '*.json' | sort | tail -n 1)
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(id) FROM deltaLakeLocal('$BASE/row_tracking_in_commit_timestamps');
    SELECT
        JSONExtractUInt(line, 'add', 'baseRowId') > 0,
        JSONExtractUInt(line, 'add', 'defaultRowCommitVersion') = toUInt64(extract('$LAST', '(\\\\d+)\\\\.json\$'))
    FROM file('$LAST', LineAsString) WHERE JSONHas(line, 'add') ORDER BY ALL;
    SELECT JSONExtractUInt(line, 'commitInfo', 'inCommitTimestamp') > 1700000000000
    FROM file('$LAST', LineAsString) WHERE JSONHas(line, 'commitInfo');
    -- The row ids of the new rows are above the high water mark of the previous commit.
    SELECT min(JSONExtractUInt(line, 'add', 'baseRowId'))
          > (SELECT JSONExtractUInt(JSONExtractString(line, 'domainMetadata', 'configuration'), 'rowIdHighWaterMark')
             FROM file('$PREVIOUS', LineAsString) WHERE JSONExtractString(line, 'domainMetadata', 'domain') = 'delta.rowTracking')
    FROM file('$LAST', LineAsString) WHERE JSONHas(line, 'add');
    -- The new high water mark is the last row id of the new rows.
    SELECT
        max(JSONExtractUInt(line, 'add', 'baseRowId') + JSONExtractUInt(JSONExtractString(line, 'add', 'stats'), 'numRecords')) - 1
          = (SELECT JSONExtractUInt(JSONExtractString(line, 'domainMetadata', 'configuration'), 'rowIdHighWaterMark')
             FROM file('$LAST', LineAsString) WHERE JSONExtractString(line, 'domainMetadata', 'domain') = 'delta.rowTracking')
    FROM file('$LAST', LineAsString) WHERE JSONHas(line, 'add')"

echo '-- unsupported features'
echo "type_widening: $(insert type_widening "(10, 1, 1, 1, 1, 1, '2020-01-01')")"
# ClickHouse writes only `numRecords` into the statistics of a data file, and clustered tables require the
# statistics of the clustering columns.
echo "liquid_clustering: $(insert liquid_clustering "(100, 'x', 1)")"
echo "iceberg_compat_v2: $(insert iceberg_compat_v2 "(100, 'x', map('a', 1), [])")"
echo "column_mapping_id: $(insert column_mapping_id "(100, 'x', (1), [], map(), 1)")"
echo "writer_features: $(insert writer_features "(100, 'x', 1, 'd')")"
for table in type_widening liquid_clustering iceberg_compat_v2 column_mapping_id writer_features
do
    ${CLICKHOUSE_CLIENT} --query "SELECT '$table', count() FROM deltaLakeLocal('$BASE/$table')"
done

rm -rf "$BASE"
