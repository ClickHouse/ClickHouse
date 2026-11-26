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

# Writes the file of a pending insert of the values $2 into a table with the structure $1 to $3.
function make_bin_file()
{
    rm -rf "$local_dir"
    mkdir -p "$local_dir"
    ${CLICKHOUSE_LOCAL} --config-file "${CLICKHOUSE_TMP}/05871_local.xml" --path "$local_dir" --query "
        CREATE TABLE data ($1) ENGINE = Null;
        CREATE TABLE dist AS data ENGINE = Distributed(test_shard_localhost, currentDatabase(), data) SETTINGS flush_on_detach = 0;
        SYSTEM STOP DISTRIBUTED SENDS dist;
        INSERT INTO dist SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0 VALUES $2;
    "
    cp "$(find "$local_dir" -path '*shard*' -name '*.bin' | head -n 1)" "$3"
}

file_name="${CLICKHOUSE_DATABASE}_05871.bin"
make_bin_file "id UInt32, s String" "(1, 'a'), (2, 'bb'), (3, 'ccc')" "${CLICKHOUSE_USER_FILES}/${file_name}"
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

# A file with a second block of another structure is broken and gives an error, not a wrong cast of the column.
# The blocks of a file with `UInt64` are added after the blocks of a file with `String`, without the header of the
# second file: the signature (5 bytes), the size of the header data, the header data and its checksum (16 bytes).
mixed_name="${CLICKHOUSE_DATABASE}_05871_mixed.bin"
make_bin_file "s String" "('a'), ('bb')" "${CLICKHOUSE_TMP}/05871_string.bin"
make_bin_file "s UInt64" "(12345678), (87654321)" "${CLICKHOUSE_TMP}/05871_uint64.bin"
read -r b0 b1 <<< "$(od -An -tu1 -j5 -N2 "${CLICKHOUSE_TMP}/05871_uint64.bin")"
if (( b0 < 128 )); then header_size=$((5 + 1 + b0 + 16)); else header_size=$((5 + 2 + ((b0 & 127) | (b1 << 7)) + 16)); fi
cp "${CLICKHOUSE_TMP}/05871_string.bin" "${CLICKHOUSE_USER_FILES}/${mixed_name}"
tail -c +$((header_size + 1)) "${CLICKHOUSE_TMP}/05871_uint64.bin" >> "${CLICKHOUSE_USER_FILES}/${mixed_name}"
rm -rf "$local_dir" "${CLICKHOUSE_TMP}/05871_local.xml" "${CLICKHOUSE_TMP}/05871_string.bin" "${CLICKHOUSE_TMP}/05871_uint64.bin"

${CLICKHOUSE_CLIENT} --query "SELECT s.size FROM file('${mixed_name}', 'Distributed')" 2>&1 | grep -o -m1 'INCORRECT_DATA'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${mixed_name}', 'Distributed') FORMAT Null" 2>&1 | grep -o -m1 'INCORRECT_DATA'
rm "${CLICKHOUSE_USER_FILES}/${mixed_name}"
