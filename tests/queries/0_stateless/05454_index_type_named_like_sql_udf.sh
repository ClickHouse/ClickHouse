#!/usr/bin/env bash

# A SQL user-defined function named like a skip index type must not be expanded in the `TYPE` of the index,
# only in its expression: an unrelated `ALTER` rebuilds the existing index and must keep working.
# SQL UDFs are server-wide, so the function named `minmax` is created in an isolated `clickhouse local`
# to not affect concurrent tests which create `minmax` indexes on the shared server.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --multiquery "
CREATE TABLE t_index_type_udf (x UInt64, INDEX idx x TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY x;
INSERT INTO t_index_type_udf VALUES (1);

CREATE FUNCTION minmax AS x -> x;

ALTER TABLE t_index_type_udf ADD COLUMN y UInt8;
SELECT * FROM t_index_type_udf;
SELECT name, type, expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_index_type_udf' ORDER BY name;
"
