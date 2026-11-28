#!/usr/bin/env bash
# EXPLAIN WHATIF of a hypothetical projection on parts with lightweight-deleted rows

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# count() must reach the read step and the real projection must be allowed to win
PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1, optimize_read_in_order = 1, prefer_optimize_projection = 0"

# the read limit counts the deleted rows the scan reads, and a part with every row deleted gets no projection part
echo "--- a part whose rows are mostly, then all deleted ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_z;
    CREATE TABLE t_est_z (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_est_z SELECT number, number % 100, number FROM numbers(1000);
    DELETE FROM t_est_z WHERE a >= 100 SETTINGS lightweight_deletes_sync = 2;
    CREATE HYPOTHETICAL PROJECTION p_z ON t_est_z (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, v FROM t_est_z WHERE a < 100 AND b < 30 SETTINGS ${PIN}, max_bytes_to_read = 10000;
    DELETE FROM t_est_z WHERE 1 SETTINGS lightweight_deletes_sync = 2;
    EXPLAIN WHATIF SELECT a, v FROM t_est_z WHERE b < 30 SETTINGS ${PIN};
" | grep -E '^\s+(status|reason|empirical_status|empirical_reason):' | awk '{$1=$1; print}'

# next to a live part, the read takes the parent's ranges of the part with every row deleted
echo "--- a part with every row deleted next to a live part ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_m; DROP TABLE IF EXISTS t_real_m;
    CREATE TABLE t_est_m (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree PARTITION BY a < 1000 ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
                 lightweight_mutation_projection_mode = 'rebuild';
    CREATE TABLE t_real_m AS t_est_m;
    ALTER TABLE t_real_m ADD PROJECTION p_m (SELECT a, b, v ORDER BY b);
    INSERT INTO t_est_m SELECT number, number % 100, number FROM numbers(2000);
    INSERT INTO t_real_m SELECT number, number % 100, number FROM numbers(2000);
    DELETE FROM t_est_m WHERE a < 1000 SETTINGS lightweight_deletes_sync = 2;
    DELETE FROM t_real_m WHERE a < 1000 SETTINGS lightweight_deletes_sync = 2;
    CREATE HYPOTHETICAL PROJECTION p_m ON t_est_m (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, v FROM t_est_m WHERE b < 30 SETTINGS ${PIN};
    -- the EXPLAIN header of the parent read counts both parts, so take what the read selects
    SELECT 'real marks:', marks FROM (EXPLAIN ESTIMATE SELECT a, v FROM t_real_m WHERE b < 30 SETTINGS ${PIN}, force_optimize_projection = 1);
" | grep -E '^\s+(marks|rows|verdict):|^real marks:' | awk '{$1=$1; print}'

# a byte-driven granularity is sized from the rows the projection keeps, not from the deleted ones read
echo "--- a mostly deleted part with a byte-driven granularity ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;
    CREATE TABLE t_est_g (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 0,
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, lightweight_mutation_projection_mode = 'rebuild';
    CREATE TABLE t_real_g AS t_est_g;
    ALTER TABLE t_real_g ADD PROJECTION p_g (SELECT a, b, v ORDER BY b);
    INSERT INTO t_est_g SELECT number, number % 100, number FROM numbers(10000);
    INSERT INTO t_real_g SELECT number, number % 100, number FROM numbers(10000);
    DELETE FROM t_est_g WHERE a % 10 != 0 SETTINGS lightweight_deletes_sync = 2;
    DELETE FROM t_real_g WHERE a % 10 != 0 SETTINGS lightweight_deletes_sync = 2;
    CREATE HYPOTHETICAL PROJECTION p_g ON t_est_g (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, v FROM t_est_g WHERE b < 30 SETTINGS ${PIN};
    SELECT 'real marks:', marks FROM (EXPLAIN ESTIMATE SELECT a, v FROM t_real_g WHERE b < 30 SETTINGS ${PIN}, force_optimize_projection = 1);
" | grep -E '^\s+(marks|rows|verdict):|^real marks:' | awk '{$1=$1; print}'

# a projection that stores `_row_exists` reads the mask once and keeps it
echo "--- a projection that stores _row_exists on a part with deleted rows ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_x;
    CREATE TABLE t_est_x (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_est_x SELECT number, number % 100 FROM numbers(1000);
    DELETE FROM t_est_x WHERE a % 2 = 0 SETTINGS lightweight_deletes_sync = 2;
    CREATE HYPOTHETICAL PROJECTION p_x ON t_est_x (SELECT a, b, _row_exists ORDER BY b);
    EXPLAIN WHATIF SELECT a, b FROM t_est_x WHERE b < 30 SETTINGS ${PIN};
" | grep -E '^\s+(marks|rows|status):' | awk '{$1=$1; print}'
