#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The bloom filters written by the C++ writer of ORC 1.6.0 - 1.6.11 and 1.7.0 use incorrect hashing (ORC-1024)
# and must be ignored, otherwise the filter pushdown skips the matching rows.
for version in 1.6.0 1.6.11
do
    for push_down in 0 1
    do
        $CLICKHOUSE_LOCAL -q "
            SELECT '$version', $push_down, id, name
            FROM file('$CUR_DIR/data_orc/bad_bloom_filter_$version.orc')
            WHERE id = 18000000000
            SETTINGS input_format_orc_filter_push_down = $push_down"
        $CLICKHOUSE_LOCAL -q "
            SELECT '$version', $push_down, count()
            FROM file('$CUR_DIR/data_orc/bad_bloom_filter_$version.orc')
            WHERE id IN (18000000000, 18000000001)
            SETTINGS input_format_orc_filter_push_down = $push_down"
    done
done
