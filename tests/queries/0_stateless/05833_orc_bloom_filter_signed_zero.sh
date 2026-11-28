#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: ORC is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Filter push down with a bloom filter keeps the rows equal to `0.0` and `-0.0` whatever the sign of the stored zero.
# The files have the values (0.0, 1.0, 2.0) and (-0.0, 1.0, 2.0) with a bloom filter, written by PyArrow.
for sign in positive negative
do
    for push_down in 0 1
    do
        $CLICKHOUSE_LOCAL -q "
            SELECT '$sign', $push_down,
                (SELECT count() FROM file('$CUR_DIR/data_orc/bloom_filter_${sign}_zero.orc') WHERE f = 0.0),
                (SELECT count() FROM file('$CUR_DIR/data_orc/bloom_filter_${sign}_zero.orc') WHERE f = -0.0)
            SETTINGS input_format_orc_filter_push_down = $push_down"
    done
done
