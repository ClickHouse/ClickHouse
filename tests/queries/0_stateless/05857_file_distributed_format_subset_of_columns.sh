#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# `file` in the `Distributed` format reads the file of a pending asynchronous insert into a `Distributed` table. A query
# that reads only some of its columns, or only virtual columns, threw a logical error, as all the columns of the file were
# returned.

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE data (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE dist AS data ENGINE = Distributed(test_shard_localhost, currentDatabase(), data);
    SYSTEM STOP DISTRIBUTED SENDS dist;
    INSERT INTO dist SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0 VALUES (1, 'hello'), (2, 'world');
"

data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'dist'")
bin_file=$(find "$data_path" -path '*shard*' -name '*.bin' | head -n 1)
file_name="${CLICKHOUSE_DATABASE}_05857.bin"
cp "$bin_file" "${CLICKHOUSE_USER_FILES}/${file_name}"
file_size=$(stat -c%s "${CLICKHOUSE_USER_FILES}/${file_name}")
file_mtime=$(stat -c%Y "${CLICKHOUSE_USER_FILES}/${file_name}")

${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM file('${file_name}', 'Distributed') ORDER BY id;
    SELECT id FROM file('${file_name}', 'Distributed') ORDER BY id;
    SELECT s, id FROM file('${file_name}', 'Distributed') ORDER BY id;
    SELECT _file = '${file_name}', _size = ${file_size}, _time = toDateTime(${file_mtime}) FROM file('${file_name}', 'Distributed');
    SELECT count() FROM file('${file_name}', 'Distributed');
    SELECT count() FROM file('${file_name}', 'Distributed') WHERE s = 'world';
"

rm "${CLICKHOUSE_USER_FILES}/${file_name}"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE dist;
    DROP TABLE data;
"
