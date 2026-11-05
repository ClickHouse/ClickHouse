#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# a column that a SQL UDF reads from the table is bound too, so its type change makes the hypothetical object not applicable
# SQL UDFs are global, so the name contains the database name
f="${CLICKHOUSE_DATABASE}_add_b"

$CLICKHOUSE_CLIENT -n -q "
SET enable_parallel_replicas = 0, optimize_use_projections = 1;
DROP FUNCTION IF EXISTS ${f};
CREATE FUNCTION ${f} AS x -> x + b;

CREATE TABLE t_udf_index (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_udf_index SELECT number, 1 FROM numbers(1000);
CREATE HYPOTHETICAL INDEX i ON t_udf_index ${f}(a) TYPE minmax;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_udf_index WHERE ${f}(a) = 42) WHERE explain LIKE '  status:%';
ALTER TABLE t_udf_index MODIFY COLUMN b Float64 SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_udf_index WHERE ${f}(a) = 42) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';

CREATE TABLE t_udf_projection (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_udf_projection SELECT number, 1 FROM numbers(1000);
CREATE HYPOTHETICAL PROJECTION p ON t_udf_projection (SELECT a, ${f}(a) AS c ORDER BY c);
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_udf_projection WHERE ${f}(a) = 42) WHERE explain LIKE '  status:%';
ALTER TABLE t_udf_projection MODIFY COLUMN b Float64 SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_udf_projection WHERE ${f}(a) = 42) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';

DROP TABLE t_udf_index;
DROP TABLE t_udf_projection;
DROP FUNCTION ${f};
"
