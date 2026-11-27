#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

echo "The first block is stored as is." | $CLICKHOUSE_COMPRESSOR --none > "${WORKING_DIR}/mixed"
echo "The second block is compressed." | $CLICKHOUSE_COMPRESSOR >> "${WORKING_DIR}/mixed"

echo "whole file"
$CLICKHOUSE_COMPRESSOR --decompress < "${WORKING_DIR}/mixed"

# Reading from an offset goes through another reader, which seeks in the file.
echo "from an offset in the first block"
$CLICKHOUSE_COMPRESSOR --decompress --offset-in-decompressed-block 4 "${WORKING_DIR}/mixed"

rm -rf "${WORKING_DIR}"
