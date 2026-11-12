#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Arrow format is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query="SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS u"

echo "default"
$CLICKHOUSE_LOCAL -q "${query} FORMAT Arrow" | $CLICKHOUSE_LOCAL --input-format Arrow -q "DESCRIBE TABLE table SETTINGS describe_compact_output = 1; SELECT u FROM table"
echo "output_format_arrow_uuid_as_fixed_byte_array = 1"
$CLICKHOUSE_LOCAL -q "${query} FORMAT Arrow SETTINGS output_format_arrow_uuid_as_fixed_byte_array = 1" \
    | $CLICKHOUSE_LOCAL --input-format Arrow -q "DESCRIBE TABLE table SETTINGS describe_compact_output = 1; SELECT hex(u) FROM table"
