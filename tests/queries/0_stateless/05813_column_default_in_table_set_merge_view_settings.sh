#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The children of a `Merge` read share the sets of the column defaults that read a table (`x IN table`), but only the
# children whose settings are the same: the contents of the set depend on them, e.g. on `transform_null_in`, which
# decides whether the set keeps `NULL`, or on `max_rows_in_set` with `set_overflow_mode = 'break'`.

file_name="${CLICKHOUSE_DATABASE}_05813.jsonl"

$CLICKHOUSE_CLIENT -m -q "
    SET enable_parallel_replicas = 0;

    DROP TABLE IF EXISTS keys_default_set_view_settings;
    DROP TABLE IF EXISTS f_default_set_view_settings;
    DROP TABLE IF EXISTS t_default_set_view_settings;

    CREATE TABLE keys_default_set_view_settings (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO keys_default_set_view_settings VALUES (NULL), (1);
    INSERT INTO keys_default_set_view_settings VALUES (2);
    INSERT INTO keys_default_set_view_settings VALUES (3);

    -- The records of the file omit the column, so its default is evaluated while reading, with the settings of the reader.
    INSERT INTO FUNCTION file('${file_name}', 'JSONEachRow', 'p Nullable(UInt64)')
        SETTINGS engine_file_truncate_on_insert = 1 VALUES (NULL), (1), (5);
    CREATE TABLE f_default_set_view_settings (p Nullable(UInt64), f Nullable(UInt8) DEFAULT p IN keys_default_set_view_settings)
        ENGINE = File(JSONEachRow, '${file_name}');

    CREATE VIEW v_default_set_view_settings_0 AS SELECT p, f FROM f_default_set_view_settings SETTINGS transform_null_in = 0;
    CREATE VIEW v_default_set_view_settings_1 AS SELECT p, f FROM f_default_set_view_settings SETTINGS transform_null_in = 1;

    SELECT 'null direct';
    SELECT p, f FROM v_default_set_view_settings_0 ORDER BY p;
    SELECT p, f FROM v_default_set_view_settings_1 ORDER BY p;

    SELECT 'null merge';
    SELECT _table, p, f FROM merge(currentDatabase(), '^v_default_set_view_settings_[01]\$') ORDER BY _table, p SETTINGS max_threads = 1;

    -- A part without the column evaluates its default with the limits of the reader.
    CREATE TABLE t_default_set_view_settings (p Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_default_set_view_settings VALUES (1), (2), (3);
    ALTER TABLE t_default_set_view_settings ADD COLUMN f Nullable(UInt8) DEFAULT p IN keys_default_set_view_settings;

    CREATE VIEW v_default_set_view_limit_0 AS SELECT p, f FROM t_default_set_view_settings;
    CREATE VIEW v_default_set_view_limit_1 AS SELECT p, f FROM t_default_set_view_settings
        SETTINGS max_rows_in_set = 1, set_overflow_mode = 'break', max_block_size = 1, max_threads = 1;

    SELECT 'limit direct';
    SELECT sum(f) FROM v_default_set_view_limit_0;
    SELECT sum(f) FROM v_default_set_view_limit_1;

    SELECT 'limit merge';
    SELECT _table, sum(f) FROM merge(currentDatabase(), '^v_default_set_view_limit_[01]\$') GROUP BY _table ORDER BY _table SETTINGS max_threads = 1;

    DROP VIEW v_default_set_view_settings_0;
    DROP VIEW v_default_set_view_settings_1;
    DROP VIEW v_default_set_view_limit_0;
    DROP VIEW v_default_set_view_limit_1;
    DROP TABLE f_default_set_view_settings;
    DROP TABLE t_default_set_view_settings;
    DROP TABLE keys_default_set_view_settings;
"
