#!/usr/bin/env bash
# Tags: linux
# Tag linux: reads `/proc/self/status`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Building the symbol index reads the symbol table of the executable and the names of ranges that have several
# symbols, and then gives those file pages back. So it must not leave more file pages resident than a later scan
# of every name maps; the slack covers a binary with `.clickhouse.symbols`, where both are small.
rss_file="SELECT toUInt64(splitByWhitespace(line)[2]) FROM file('/proc/self/status', LineAsString) WHERE startsWith(line, 'RssFile:')"

read -r before_index after_index after_scan <<< "$($CLICKHOUSE_LOCAL -q "
    $rss_file;
    SELECT addressToSymbol(toUInt64(1)) FORMAT Null SETTINGS allow_introspection_functions = 1;
    $rss_file;
    SELECT count() FROM system.symbols WHERE NOT ignore(symbol) FORMAT Null SETTINGS allow_introspection_functions = 1;
    $rss_file;
" | tr '\n' ' ')"

slack_kib=$((32 * 1024))
if [ -n "$after_scan" ] && [ $((after_index - before_index)) -le $((after_scan - after_index + slack_kib)) ]; then
    echo "OK"
else
    echo "RssFile in KiB before the index, after the index, after the scan: '$before_index' '$after_index' '$after_scan'"
fi
