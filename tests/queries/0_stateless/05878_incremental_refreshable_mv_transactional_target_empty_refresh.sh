#!/usr/bin/env bash
# Tags: no-fasttest
# A target that commits the cursor of an incremental refresh along with its data (`Iceberg`) has no data to commit when
# the query of the refresh filters out all the new rows of the source. These rows are still consumed, so the cursor is
# committed in a snapshot without data files. A refresh that does not advance the cursor adds no snapshot.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_transactional_target_empty_refresh/"
rm -rf "${TABLE_PATH}"

# The number of snapshots of the target and the partitions of the source in the cursor of its current snapshot.
cursor_state()
{
    python3 -I - "${TABLE_PATH}metadata" <<'PYEOF'
import base64, json, os, re, sys

metadata_dir = sys.argv[1]
versions = {int(re.match(r'v?(\d+)', name).group(1)): name for name in os.listdir(metadata_dir) if name.endswith('.metadata.json')}
with open(os.path.join(metadata_dir, versions[max(versions)])) as f:
    metadata = json.load(f)
snapshot = next(s for s in metadata['snapshots'] if s['snapshot-id'] == metadata['current-snapshot-id'])
cursor = base64.b64decode(snapshot['summary']['clickhouse.refresh-cursor'])
partitions = ' '.join(sorted({p.decode() for p in re.findall(rb'(\d+)\.block_number', cursor)}))
print('snapshots:', len(metadata['snapshots']), 'cursor partitions:', partitions)
PYEOF
}

refresh()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv;"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE src (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    CREATE TABLE tgt (k Int64) ENGINE = IcebergLocal('${TABLE_PATH}');
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT k FROM src WHERE k > 0;
    INSERT INTO src VALUES (1, 1);
"
refresh
cursor_state

# The new rows are filtered out: nothing is appended, but the cursor advances to them.
${CLICKHOUSE_CLIENT} --query "INSERT INTO src VALUES (2, 0)"
refresh
cursor_state

# Nothing new: no snapshot is added.
refresh
cursor_state

${CLICKHOUSE_CLIENT} --query "INSERT INTO src VALUES (3, 3)"
refresh
cursor_state
${CLICKHOUSE_CLIENT} --query "SELECT k FROM tgt ORDER BY k; DROP TABLE mv; DROP TABLE tgt; DROP TABLE src;"
rm -rf "${TABLE_PATH}"
