#!/usr/bin/env bash
# Tags: no-fasttest
# The native Arrow IPC reader converts big-endian files and streams to little-endian before decoding. The files are
# the big-endian integration files of https://github.com/apache/arrow-testing (`data/arrow-ipc-stream/integration/
# 1.0.0-bigendian` at commit 9ff285c88565f0f6abc855918c6a342e70e4909c); the two decimal streams are cut to their schema
# and first record batch. The reference is the output for the little-endian versions of the same files: the
# values, and a hash of every row.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for FILE in "$CUR_DIR"/data_arrow/big_endian/*
do
    NAME=$(basename "$FILE")
    FORMAT=ArrowStream
    [[ $NAME == *.arrow_file ]] && FORMAT=Arrow
    echo "--- $NAME"
    ${CLICKHOUSE_LOCAL} --session_timezone UTC --query "
        DESC file('$FILE', $FORMAT);
        SELECT * FROM file('$FILE', $FORMAT) LIMIT 2;
        SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM file('$FILE', $FORMAT);"
done
