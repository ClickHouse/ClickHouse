#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# no-fasttest: Substrait is not built in fast test
# no-random-settings: the test needs one certain join order, and random settings shuffle it

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --enable_substrait 1 --use_hash_table_stats_for_join_reordering 0 --ignore-error -q "
    DROP TABLE IF EXISTS t_substrait_semi_a;
    DROP TABLE IF EXISTS t_substrait_semi_b;
    DROP TABLE IF EXISTS t_substrait_semi_c;
    CREATE TABLE t_substrait_semi_a (id Int64, k Int64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_substrait_semi_b (k Int64, w Int64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_substrait_semi_c (id Int64, w Int64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_substrait_semi_a SELECT number, number FROM numbers(100);
    INSERT INTO t_substrait_semi_b SELECT number, number FROM numbers(2);
    INSERT INTO t_substrait_semi_c SELECT number, number FROM numbers(10000);

    EXPLAIN SUBSTRAIT SELECT a.id FROM t_substrait_semi_a AS a LEFT SEMI JOIN t_substrait_semi_c AS c ON a.id = c.id INNER JOIN t_substrait_semi_b AS b ON a.k = b.k AND b.w > c.w;

    DROP TABLE t_substrait_semi_a;
    DROP TABLE t_substrait_semi_b;
    DROP TABLE t_substrait_semi_c;
" 2>&1 | sed -E '/^Received exception/d; /^\(query:/d; s/^Code: .*DB::Exception: //; s/\. \(NOT_IMPLEMENTED\)$//'
