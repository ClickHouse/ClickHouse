#!/usr/bin/env bash
# Tags: no-fasttest
# A target that commits the cursor of an incremental refresh along with its data (`Iceberg`) does it when its input
# is finished. A query that stops the read early (a `DISTINCT` by a constant) finishes its output when one of the
# parallel streams of the source gives the row, while the other streams report their cursors later, from their own
# shutdown. The target must commit the cursor only after all of them, so that a refresh without new rows in the source
# appends nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_early_stop_transactional_target/"
rm -rf "${TABLE_PATH}"

# The number of partitions of the source in the refresh cursor that the current Iceberg snapshot of the target stores.
cursor_partitions()
{
    python3 -I - "${TABLE_PATH}metadata" <<'PYEOF'
import base64, json, os, re, sys

metadata_dir = sys.argv[1]
versions = {int(re.match(r'v?(\d+)', name).group(1)): name for name in os.listdir(metadata_dir) if name.endswith('.metadata.json')}
with open(os.path.join(metadata_dir, versions[max(versions)])) as f:
    metadata = json.load(f)
snapshot = next(s for s in metadata['snapshots'] if s['snapshot-id'] == metadata['current-snapshot-id'])
cursor = base64.b64decode(snapshot['summary']['clickhouse.refresh-cursor'])
print('cursor partitions:', len({p for p in re.findall(rb'(\d+)\.block_number', cursor)}))
PYEOF
}

refresh()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv;"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE src (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    INSERT INTO src SELECT number % 16, number FROM numbers(160000);
    CREATE TABLE tgt (c UInt8) ENGINE = IcebergLocal('${TABLE_PATH}');
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT DISTINCT 1 AS c FROM src
        SETTINGS max_block_size = 1, max_threads = 16;
"
refresh
cursor_partitions
${CLICKHOUSE_CLIENT} --query "SELECT 'first', count() FROM tgt"

# No new rows in the source: nothing is appended.
refresh
refresh
refresh
${CLICKHOUSE_CLIENT} --query "SELECT 'idle', count() FROM tgt"

# New rows in one partition: one more row.
${CLICKHOUSE_CLIENT} --query "INSERT INTO src VALUES (3, 1000000)"
refresh
cursor_partitions
${CLICKHOUSE_CLIENT} --query "SELECT 'new', count() FROM tgt"
refresh
${CLICKHOUSE_CLIENT} --query "SELECT 'idle', count() FROM tgt; DROP TABLE mv; DROP TABLE tgt; DROP TABLE src;"
rm -rf "${TABLE_PATH}"
