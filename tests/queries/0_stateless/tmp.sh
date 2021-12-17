#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02149.data
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

echo "MsgPack"

$CLICKHOUSE_CLIENT -q "select toInt32(number % 2 ? number : NULL) as int, toUInt64(number % 2 ? NULL : number) as uint, toFloat32(number) as float, concat('Str: ', toString(number)) as str, [[number, number + 1], [number]] as arr, map(number, [number, number + 1]) as map from numbers(3) format MsgPack" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'MsgPack') settings input_format_msgpack_number_of_columns=6"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'MsgPack') settings input_format_msgpack_number_of_columns=6"


rm $SCHEMADIR/resultset_format_02149 $SCHEMADIR/row_format_02149
rm $DATA_FILE

