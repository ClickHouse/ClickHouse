#!/usr/bin/env bash
# Tags: no-fasttest
# An incremental refresh starts from the cursor of the previous refresh, but a read of the source replaces its part
# of that cursor: the partitions that were dropped from the source must be retired from the cursor instead of
# accumulating in it forever, while the positions of the remaining partitions are kept.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_cursor_retires_partitions/"
rm -rf "${TABLE_PATH}"

# The partitions in the refresh cursor that the current Iceberg snapshot of the target stores.
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
print('cursor partitions:', ' '.join(sorted({p.decode() for p in re.findall(rb'(\d+)\.block_number', cursor)})))
PYEOF
}

refresh()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv;"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE src (p Int64, id Int64) ENGINE = MergeTree PARTITION BY p ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    CREATE TABLE tgt (p Int64, id Int64) ENGINE = IcebergLocal('${TABLE_PATH}');
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT p, id FROM src;
    INSERT INTO src VALUES (101, 1), (202, 2);
"
refresh
cursor_partitions

# Cycle through the partitions: each refresh sees one partition dropped and a new one added.
for step in 3 4 5
do
    ${CLICKHOUSE_CLIENT} --query "
        ALTER TABLE src DROP PARTITION $(( (step - 2) * 101 ));
        INSERT INTO src VALUES ($(( step * 101 )), ${step});
    "
    refresh
    cursor_partitions
done

# Nothing new: the remaining partitions keep their position, so nothing is appended again.
refresh
cursor_partitions
${CLICKHOUSE_CLIENT} --query "SELECT p, id FROM tgt ORDER BY ALL; DROP TABLE mv; DROP TABLE tgt; DROP TABLE src;"
rm -rf "${TABLE_PATH}"
