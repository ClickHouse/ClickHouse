#!/usr/bin/env bash

# A SQL UDF called in a `Tuple` element `DEFAULT` must be expanded before the default is pulled up to
# the column level, the same as in a column-level `DEFAULT`: after `DROP FUNCTION` the table must keep working.
# SQL UDFs are global, so the function name includes the database name to keep concurrent runs independent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

f="f_05449_tuple_default_udf_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_tuple_default_udf;
DROP FUNCTION IF EXISTS ${f};

CREATE FUNCTION ${f} AS (x) -> x + 1;

CREATE TABLE t_tuple_default_udf
(
    id UInt8,
    c Tuple(a UInt8 DEFAULT ${f}(id))
)
ENGINE = Memory;

ALTER TABLE t_tuple_default_udf ADD COLUMN d Tuple(a UInt8 DEFAULT ${f}(id + 10));
ALTER TABLE t_tuple_default_udf ADD COLUMN e Tuple(a UInt8);
ALTER TABLE t_tuple_default_udf MODIFY COLUMN e Tuple(a UInt8 DEFAULT ${f}(id + 20));

DROP FUNCTION ${f};

SELECT name, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_udf' ORDER BY position;

INSERT INTO t_tuple_default_udf (id) VALUES (2);

SELECT * FROM t_tuple_default_udf;

DROP TABLE t_tuple_default_udf;
"
