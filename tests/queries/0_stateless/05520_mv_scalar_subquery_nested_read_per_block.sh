#!/usr/bin/env bash

# A scalar subquery of a materialized view that reads the source table through `loop`, `merge`, `executable`
# or a table wrapping them reads the inserted block, so each block of a multi-block INSERT gets its own value.
# The database name is spelled out: `currentDatabase()` in the view query does not resolve to the test database
# during the push.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ONE_ROW_BLOCKS="max_threads = 1, max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0,
    min_insert_block_size_rows_for_materialized_views = 0, min_insert_block_size_bytes_for_materialized_views = 0"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1};
DROP TABLE IF EXISTS mv_loop;
DROP TABLE IF EXISTS mv_loop_table;
DROP TABLE IF EXISTS mv_merge;
DROP TABLE IF EXISTS mv_alias;
DROP TABLE IF EXISTS mv_parallel;
DROP TABLE IF EXISTS dst_parallel;
DROP TABLE IF EXISTS src_null;
DROP TABLE IF EXISTS src_loop_alias;
DROP TABLE IF EXISTS src_loop;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;

SET allow_experimental_alias_table_engine = 1;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst (name String, inserted UInt64, seen Nullable(UInt64)) ENGINE = MergeTree ORDER BY (name, inserted);
CREATE TABLE src_loop AS loop(${CLICKHOUSE_DATABASE}, src);
CREATE TABLE src_loop_alias ENGINE = Alias(${CLICKHOUSE_DATABASE}, src_loop);

CREATE MATERIALIZED VIEW mv_loop TO dst AS SELECT 'loop' AS name, x AS inserted,
    (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) LIMIT 10)) AS seen FROM src;
CREATE MATERIALIZED VIEW mv_loop_table TO dst AS SELECT 'loop_table' AS name, x AS inserted,
    (SELECT max(x) FROM (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_loop LIMIT 10)) AS seen FROM src;
CREATE MATERIALIZED VIEW mv_merge TO dst AS SELECT 'merge' AS name, x AS inserted,
    (SELECT max(x) FROM (SELECT x FROM merge(${CLICKHOUSE_DATABASE}, '^src_loop\$') LIMIT 10)) AS seen FROM src;
CREATE MATERIALIZED VIEW mv_alias TO dst AS SELECT 'alias' AS name, x AS inserted,
    (SELECT max(x) FROM (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_loop_alias LIMIT 10)) AS seen FROM src;

INSERT INTO src SELECT number + 1 FROM numbers(3) SETTINGS ${ONE_ROW_BLOCKS};

SELECT 'insert', name, groupArray((inserted, seen)) FROM (SELECT * FROM dst ORDER BY name, inserted) GROUP BY name ORDER BY name;

-- A refreshable view appending to the source table pushes its blocks to these views like an INSERT. It lives in an
-- Atomic database: in a Replicated one an APPEND refresh runs on one replica only.
TRUNCATE TABLE dst;
CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Atomic;
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append REFRESH EVERY 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.src EMPTY AS
    SELECT number + 4 AS x FROM numbers(3) SETTINGS ${ONE_ROW_BLOCKS};
SYSTEM REFRESH VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append;
SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append;

SELECT 'append_refresh', name, groupArray((inserted, seen)) FROM (SELECT * FROM dst ORDER BY name, inserted) GROUP BY name ORDER BY name;

-- A refreshable view reading the source table has no inserted block, so it reads the table.
TRUNCATE TABLE dst;
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read REFRESH EVERY 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.dst EMPTY AS
    SELECT 'loop' AS name, max(x) AS inserted,
        (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) LIMIT 10)) AS seen FROM ${CLICKHOUSE_DATABASE}.src
    UNION ALL
    SELECT 'loop_table', max(x), (SELECT max(x) FROM (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_loop LIMIT 10)) FROM ${CLICKHOUSE_DATABASE}.src
    UNION ALL
    SELECT 'merge', max(x), (SELECT max(x) FROM (SELECT x FROM merge(${CLICKHOUSE_DATABASE}, '^src_loop\$') LIMIT 10)) FROM ${CLICKHOUSE_DATABASE}.src
    UNION ALL
    SELECT 'alias', max(x), (SELECT max(x) FROM (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_loop_alias LIMIT 10)) FROM ${CLICKHOUSE_DATABASE}.src;
SYSTEM REFRESH VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read;
SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read;

SELECT 'refreshable', name, groupArray((inserted, seen)) FROM (SELECT * FROM dst ORDER BY name, inserted) GROUP BY name ORDER BY name;

-- Parallel view branches of one INSERT: an Array or Tuple scalar of one block must not take the value of another block.
CREATE TABLE src_null (x UInt64) ENGINE = Null;
CREATE TABLE dst_parallel (x UInt64, seen_array Array(UInt64), seen_tuple Tuple(UInt64, String), seen_source Array(UInt64)) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_parallel TO dst_parallel AS SELECT x,
    (SELECT groupArray(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src_null) LIMIT 1)) AS seen_array,
    (SELECT max(x), any(toString(x)) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src_null) LIMIT 1)) AS seen_tuple,
    (SELECT groupArray(x) FROM ${CLICKHOUSE_DATABASE}.src_null) AS seen_source FROM src_null;

INSERT INTO src_null SELECT number FROM numbers(200) SETTINGS max_insert_threads = 8, max_threads = 8, parallel_view_processing = 1,
    max_insert_threads_min_free_memory_per_thread = 0, max_threads_min_free_memory_per_thread = 0, enable_scalar_subquery_optimization = 1,
    max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0,
    min_insert_block_size_rows_for_materialized_views = 0, min_insert_block_size_bytes_for_materialized_views = 0;

SELECT 'parallel', count(), countIf(seen_array != [x]), countIf(seen_tuple != (x, toString(x))), countIf(seen_source != [x]) FROM dst_parallel;

DROP DATABASE ${CLICKHOUSE_DATABASE_1};
DROP TABLE mv_loop;
DROP TABLE mv_loop_table;
DROP TABLE mv_merge;
DROP TABLE mv_alias;
DROP TABLE mv_parallel;
DROP TABLE dst_parallel;
DROP TABLE src_null;
DROP TABLE src_loop_alias;
DROP TABLE src_loop;
DROP TABLE dst;
DROP TABLE src;
"

# The input query of `executable` needs `user_scripts_path`, which is a server config option.
WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05520_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT
mkdir -p "${WORK_DIR}/user_scripts" "${WORK_DIR}/data"

cat > "${WORK_DIR}/user_scripts/echo_rows.sh" <<'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r x; do
    printf '%s\n' "${x}"
done
SCRIPT
chmod +x "${WORK_DIR}/user_scripts/echo_rows.sh"

cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/user_scripts/</user_scripts_path>
    <path>${WORK_DIR}/data/</path>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" -m --query "
CREATE DATABASE db_05520;
CREATE TABLE db_05520.src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db_05520.dst (name String, inserted UInt64, seen Nullable(UInt64)) ENGINE = MergeTree ORDER BY (name, inserted);
CREATE TABLE db_05520.exe (x UInt64) ENGINE = Executable('echo_rows.sh', 'TSV', (SELECT x FROM db_05520.src));

CREATE MATERIALIZED VIEW db_05520.mv_executable TO db_05520.dst AS SELECT 'executable' AS name, x AS inserted,
    (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05520.src))) AS seen FROM db_05520.src;
CREATE MATERIALIZED VIEW db_05520.mv_loop_executable_table TO db_05520.dst AS SELECT 'loop_executable_table' AS name, x AS inserted,
    (SELECT max(x) FROM (SELECT x FROM loop(db_05520, exe) LIMIT 10)) AS seen FROM db_05520.src;

INSERT INTO db_05520.src SELECT number + 1 FROM numbers(3) SETTINGS ${ONE_ROW_BLOCKS};

SELECT 'insert', name, groupArray((inserted, seen)) FROM (SELECT * FROM db_05520.dst ORDER BY name, inserted) GROUP BY name ORDER BY name;

TRUNCATE TABLE db_05520.dst;
CREATE MATERIALIZED VIEW db_05520.rmv_append REFRESH EVERY 1 YEAR APPEND TO db_05520.src EMPTY AS
    SELECT number + 4 AS x FROM numbers(3) SETTINGS ${ONE_ROW_BLOCKS};
SYSTEM REFRESH VIEW db_05520.rmv_append;
SYSTEM WAIT VIEW db_05520.rmv_append;

SELECT 'append_refresh', name, groupArray((inserted, seen)) FROM (SELECT * FROM db_05520.dst ORDER BY name, inserted) GROUP BY name ORDER BY name;

TRUNCATE TABLE db_05520.dst;
CREATE MATERIALIZED VIEW db_05520.rmv_read REFRESH EVERY 1 YEAR APPEND TO db_05520.dst EMPTY AS
    SELECT 'executable' AS name, max(x) AS inserted,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05520.src))) AS seen FROM db_05520.src
    UNION ALL
    SELECT 'loop_executable_table', max(x), (SELECT max(x) FROM (SELECT x FROM loop(db_05520, exe) LIMIT 10)) FROM db_05520.src;
SYSTEM REFRESH VIEW db_05520.rmv_read;
SYSTEM WAIT VIEW db_05520.rmv_read;

SELECT 'refreshable', name, groupArray((inserted, seen)) FROM (SELECT * FROM db_05520.dst ORDER BY name, inserted) GROUP BY name ORDER BY name;
"
