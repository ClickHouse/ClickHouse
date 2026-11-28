#!/usr/bin/env bash

# Lightweight updates work only if `enable_block_number_column` and `enable_block_offset_column` are enabled
# in the definition of the table. `CREATE TABLE` writes them there if the server configuration enables them,
# while a table created before that does not get lightweight updates from the server configuration alone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

CONFIG="${DATA_DIR}/config.xml"
echo "<clickhouse>
    <merge_tree>
        <enable_block_number_column>1</enable_block_number_column>
        <enable_block_offset_column>1</enable_block_offset_column>
    </merge_tree>
</clickhouse>" > "${CONFIG}"

echo "Tables created while the server configuration does not enable the settings"
$CLICKHOUSE_LOCAL --path "${DATA_DIR}/data" --query "
    CREATE TABLE t_old (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_old SELECT number, 'a' FROM numbers(3);
    CREATE TABLE t_old_empty (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
    SELECT name, extractAll(engine_full, 'enable_block_\\\\w+_column = \\\\w+') FROM system.tables WHERE database = currentDatabase() ORDER BY name;
"

echo "The server configuration enables the settings"
$CLICKHOUSE_LOCAL --path "${DATA_DIR}/data" --config-file "${CONFIG}" --enable_lightweight_update 1 --query "
    CREATE TABLE t_new (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_new_explicit (id UInt64, v String) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 0;
    SELECT name, extractAll(engine_full, 'enable_block_\\\\w+_column = \\\\w+') FROM system.tables WHERE database = currentDatabase() ORDER BY name;

    -- A new table has the settings in its definition, so lightweight updates work.
    INSERT INTO t_new SELECT number, 'a' FROM numbers(3);
    UPDATE t_new SET v = 'updated' WHERE id = 1;
    SELECT 't_new', * FROM t_new ORDER BY id;

    -- An old table has them only from the server configuration.
    UPDATE t_old SET v = 'updated' WHERE id = 1; -- { serverError NOT_IMPLEMENTED }
    -- And they cannot be written to its definition, because the table has data.
    ALTER TABLE t_old MODIFY SETTING enable_block_number_column = 1; -- { serverError BAD_ARGUMENTS }
    ALTER TABLE t_old MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }
    ALTER TABLE t_old ADD COLUMN c UInt8;
    SELECT 't_old', * FROM t_old ORDER BY id;

    -- An empty table can get them.
    UPDATE t_old_empty SET v = 'updated' WHERE id = 1; -- { serverError NOT_IMPLEMENTED }
    ALTER TABLE t_old_empty MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1;
    INSERT INTO t_old_empty SELECT number, 'a' FROM numbers(3);
    UPDATE t_old_empty SET v = 'updated' WHERE id = 1;
    SELECT 't_old_empty', * FROM t_old_empty ORDER BY id;
"

rm -rf "${DATA_DIR}"
