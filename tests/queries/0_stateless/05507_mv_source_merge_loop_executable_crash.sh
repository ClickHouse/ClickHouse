#!/usr/bin/env bash

# Inside a materialized view query, the source table read by the nested interpreter of `loop` or `executable`
# is the inserted block, like the rest of the view query, while a `merge` child reads the table whatever its
# siblings are. The database name is spelled out: `currentDatabase()` in the view query does not resolve to the
# test database during the push.
# A refreshable view appending to the source table behaves like an INSERT; a refreshable view reading
# the source table reads the table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1};
DROP TABLE IF EXISTS mv_merge;
DROP TABLE IF EXISTS mv_merge_policy;
DROP ROW POLICY IF EXISTS policy_05507 ON child;
DROP TABLE IF EXISTS child;
DROP TABLE IF EXISTS mv_loop;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS dist_target;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number + 100 FROM numbers(10);

CREATE TABLE dist_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist AS dist_target ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, dist_target);

CREATE TABLE dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

-- 'dist' raises the merge read above FetchColumns, so 'src' is planned by a nested interpreter.
CREATE MATERIALIZED VIEW mv_merge TO dst AS
    SELECT 'merge' AS name, count() AS block_rows,
        (SELECT sum(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$') WHERE x > 0) AS value
    FROM src;

-- Another child planned by the nested interpreter keeps reading the inserted block in its row policy, as at FetchColumns.
CREATE TABLE child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO child VALUES (1), (4), (100);
CREATE ROW POLICY OR REPLACE policy_05507 ON child USING x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src) TO ALL;
CREATE MATERIALIZED VIEW mv_merge_policy TO dst AS
    SELECT 'merge_policy' AS name, count() AS block_rows,
        (SELECT sum(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(child|dist)\$')) AS value
    FROM src;

CREATE MATERIALIZED VIEW mv_loop TO dst AS
    SELECT 'loop' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) WHERE x > 0 LIMIT 25)) AS value
    FROM src;

INSERT INTO src VALUES (1), (2), (3);

SELECT 'insert', * FROM dst ORDER BY name;

-- A refreshable view appending to the source table pushes its block to these views like an INSERT. It lives in an Atomic
-- database: in a Replicated one an APPEND refresh runs on one replica only.
TRUNCATE TABLE dst;
CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Atomic;
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append REFRESH EVERY 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.src EMPTY AS
    SELECT number + 4 AS x FROM numbers(3);
SYSTEM REFRESH VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append;
SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append;

SELECT 'append_refresh', * FROM dst ORDER BY name;

-- A refreshable view reading the source table has no inserted block, so it reads the table.
TRUNCATE TABLE dst;
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read REFRESH EVERY 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.dst EMPTY AS
    SELECT 'merge' AS name, count() AS block_rows,
        (SELECT sum(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$') WHERE x > 0) AS value
    FROM ${CLICKHOUSE_DATABASE}.src
    UNION ALL
    SELECT 'loop', count(), (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) WHERE x > 0 LIMIT 25))
    FROM ${CLICKHOUSE_DATABASE}.src;
SYSTEM REFRESH VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read;
SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read;

SELECT 'refreshable', * FROM dst ORDER BY name;

DROP DATABASE ${CLICKHOUSE_DATABASE_1};
DROP TABLE mv_merge;
DROP TABLE mv_merge_policy;
DROP ROW POLICY policy_05507 ON child;
DROP TABLE child;
DROP TABLE mv_loop;
DROP TABLE dst;
DROP TABLE dist;
DROP TABLE dist_target;
DROP TABLE src;
"

# The input query of `executable` needs `user_scripts_path`, which is a server config option.
WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05507_XXXXXX")
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
CREATE DATABASE db_05507;
CREATE TABLE db_05507.src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05507.src SELECT number + 100 FROM numbers(10);
CREATE TABLE db_05507.dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

CREATE MATERIALIZED VIEW db_05507.mv_executable TO db_05507.dst AS
    SELECT 'executable' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05507.src WHERE x > 0))) AS value
    FROM db_05507.src;

INSERT INTO db_05507.src VALUES (1), (2), (3);

SELECT 'insert', * FROM db_05507.dst ORDER BY name;

TRUNCATE TABLE db_05507.dst;
CREATE MATERIALIZED VIEW db_05507.rmv_append REFRESH EVERY 1 YEAR APPEND TO db_05507.src EMPTY AS SELECT number + 4 AS x FROM numbers(3);
SYSTEM REFRESH VIEW db_05507.rmv_append;
SYSTEM WAIT VIEW db_05507.rmv_append;

SELECT 'append_refresh', * FROM db_05507.dst ORDER BY name;

TRUNCATE TABLE db_05507.dst;
CREATE MATERIALIZED VIEW db_05507.rmv_read REFRESH EVERY 1 YEAR APPEND TO db_05507.dst EMPTY AS
    SELECT 'executable' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05507.src WHERE x > 0))) AS value
    FROM db_05507.src;
SYSTEM REFRESH VIEW db_05507.rmv_read;
SYSTEM WAIT VIEW db_05507.rmv_read;

SELECT 'refreshable', * FROM db_05507.dst ORDER BY name;
"
