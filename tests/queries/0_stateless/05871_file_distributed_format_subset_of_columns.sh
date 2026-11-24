#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading some of the columns, the columns in another order or only virtual columns of a file in the `Distributed`
# format gave a logical error or a crash, and `_size`, `_time` and `_row_number` were NULL.

# The file of a pending asynchronous insert is made by `clickhouse-local` in its own directory, so the test does not
# touch the data of the server.
local_dir="${CLICKHOUSE_TMP}/05871_local"
rm -rf "$local_dir"
mkdir -p "$local_dir"
cat > "${CLICKHOUSE_TMP}/05871_local.xml" <<EOF
<clickhouse>
    <remote_servers>
        <test_shard_localhost>
            <shard><replica><host>localhost</host><port>9000</port></replica></shard>
        </test_shard_localhost>
    </remote_servers>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "${CLICKHOUSE_TMP}/05871_local.xml" --path "$local_dir" --query "
    CREATE TABLE data (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE dist AS data ENGINE = Distributed(test_shard_localhost, currentDatabase(), data) SETTINGS flush_on_detach = 0;
    SYSTEM STOP DISTRIBUTED SENDS dist;
    INSERT INTO dist SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc');
"

bin_file=$(find "$local_dir" -path '*shard*' -name '*.bin' | head -n 1)
file_name="${CLICKHOUSE_DATABASE}_05871.bin"
cp "$bin_file" "${CLICKHOUSE_USER_FILES}/${file_name}"
rm -rf "$local_dir" "${CLICKHOUSE_TMP}/05871_local.xml"
file_size=$(wc -c < "${CLICKHOUSE_USER_FILES}/${file_name}" | tr -d ' ')
file_mtime=$(date -r "${CLICKHOUSE_USER_FILES}/${file_name}" +%s)

${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM file('${file_name}', 'Distributed');
    SELECT id FROM file('${file_name}', 'Distributed');
    SELECT s, id FROM file('${file_name}', 'Distributed');
    SELECT s, _file = '${file_name}', id FROM file('${file_name}', 'Distributed');
    SELECT s.size, _file = '${file_name}' FROM file('${file_name}', 'Distributed');
    SELECT count() FROM file('${file_name}', 'Distributed');
    SELECT count() FROM file('${file_name}', 'Distributed') WHERE s = 'bb';
    SELECT id, _row_number, _size = ${file_size}, _time = toDateTime(${file_mtime}) FROM file('${file_name}', 'Distributed');
    CREATE TABLE file_table (id UInt32, s String) ENGINE = File('Distributed', '${file_name}');
    SELECT s FROM file_table;
"

rm "${CLICKHOUSE_USER_FILES}/${file_name}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE file_table"
