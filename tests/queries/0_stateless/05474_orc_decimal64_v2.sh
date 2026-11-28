#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `decimal64_v2.orc` from the examples of apache/orc has the format version 1.9999 (`UNSTABLE_PRE_2_0`),
# where the DECIMAL columns with the precision up to 18 are signed RLEv2 integers without the SECONDARY stream.
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$CUR_DIR/data_orc/decimal64_v2.orc', ORC)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_orc/decimal64_v2.orc', ORC)"
$CLICKHOUSE_LOCAL -q "SELECT b, d FROM file('$CUR_DIR/data_orc/decimal64_v2.orc', ORC) WHERE b > 30 SETTINGS input_format_orc_filter_push_down = 1"
