#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The exception about an unsupported type names the type and the column.

$CLICKHOUSE_CLIENT --query "SELECT * FROM obfuscate(SELECT 1 AS ok, [toIntervalDay(1)] AS bad) LIMIT 1" 2>&1 \
    | grep -o -F -e 'NOT_IMPLEMENTED' -e 'Obfuscation is not implemented for data type IntervalDay' -e 'column bad of type Array(IntervalDay)' | LC_ALL=C sort -u
