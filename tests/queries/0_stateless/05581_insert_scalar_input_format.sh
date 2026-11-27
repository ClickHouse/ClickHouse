#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

${CLICKHOUSE_CLIENT} --query="CREATE TABLE scalar_input_format (x UInt64) ENGINE = Memory"

for format in Null TSV; do
    ${CLICKHOUSE_CLIENT} --async_insert=0 --query="
        INSERT INTO scalar_input_format WITH (v -> v + 1) AS input SELECT input(10) FORMAT ${format};"
    ${CLICKHOUSE_CLIENT} --query="SELECT count(), sum(x) FROM scalar_input_format"
    ${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE scalar_input_format"
done

${CLICKHOUSE_CLIENT} --query="
    EXPLAIN SYNTAX INSERT INTO scalar_input_format
    WITH (v -> v + 1) AS input SELECT input(10) FORMAT TSV;" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP TABLE scalar_input_format"
