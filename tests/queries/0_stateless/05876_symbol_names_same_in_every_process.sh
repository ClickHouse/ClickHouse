#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Several symbols can share one address range (aliases, functions folded by the linker), and `system.symbols`
# lists one name for each range. Two processes of the same binary have to report the same name for every range.
# `address_begin` and `address_end` are offsets inside the executable, so the fingerprint does not depend on the
# address the executable is loaded at. A sum of per-row hashes does not depend on the order of the rows.
query="
    SELECT count(), sum(cityHash64(address_begin, address_end, symbol))
    FROM system.symbols
    SETTINGS allow_introspection_functions = 1
    FORMAT TSV"

first=$($CLICKHOUSE_LOCAL -q "$query")
second=$($CLICKHOUSE_LOCAL -q "$query")

read -r rows _ <<< "$first"
if [ "${rows:-0}" -gt 0 ] && [ "$first" = "$second" ]; then
    echo "OK"
else
    echo "Rows and fingerprint of system.symbols in two processes: '$first' and '$second'"
fi
