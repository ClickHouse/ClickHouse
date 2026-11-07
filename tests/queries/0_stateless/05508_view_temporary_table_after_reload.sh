#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_1"
RDB="${CLICKHOUSE_DATABASE}_2"

# CREATE leaves a name that resolves to a temporary table or view of the session unqualified,
# also when the database has a table of that name.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --query "
CREATE DATABASE ${DB} ENGINE = Atomic;
CREATE DATABASE ${RDB} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb', 'shard1', 'replica1');
CREATE TABLE ${CLICKHOUSE_DATABASE}.tmp_shadow (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.tmp_shadow VALUES (9);
USE ${DB};
CREATE TABLE ${DB}.tmp_shadow (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB}.tmp_shadow VALUES (2);
CREATE TABLE ${DB}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.mv ENGINE = MergeTree ORDER BY id AS SELECT id + 2 AS id FROM ${DB}.src;
INSERT INTO ${DB}.src VALUES (1);
CREATE MATERIALIZED VIEW ${DB}.rmv REFRESH EVERY 1 YEAR ENGINE = MergeTree ORDER BY id AS SELECT id + 3 AS id FROM ${DB}.src;
SYSTEM WAIT VIEW ${DB}.rmv;
CREATE DICTIONARY ${DB}.dict (id UInt64, val UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(QUERY 'SELECT 1 AS id, 5 AS val')) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE ${DB}.rt (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rt', '1') ORDER BY id;
INSERT INTO ${DB}.rt VALUES (6);
CREATE TABLE ${RDB}.rsrc (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id;
INSERT INTO ${RDB}.rsrc VALUES (7);
CREATE TEMPORARY TABLE tmp_src (id UInt64);
CREATE TEMPORARY TABLE tmp_shadow (id UInt64);
CREATE TEMPORARY VIEW tmp_view AS SELECT id FROM tmp_src;
CREATE VIEW ${DB}.v_from AS SELECT id FROM tmp_src;
CREATE VIEW ${DB}.v_join AS SELECT n.number AS id FROM numbers(3) AS n INNER JOIN tmp_src AS t ON n.number = t.id;
CREATE VIEW ${DB}.v_in AS SELECT number AS id FROM numbers(3) WHERE number IN tmp_src;
CREATE VIEW ${DB}.v_scalar AS SELECT (SELECT max(id) FROM tmp_src) AS id;
CREATE VIEW ${DB}.v_shadow AS SELECT id FROM tmp_shadow;
CREATE VIEW ${DB}.v_tview AS SELECT id FROM tmp_view;
CREATE VIEW ${DB}.v_view AS SELECT id FROM v_from;
CREATE VIEW ${DB}.v_mv AS SELECT id FROM mv;
CREATE VIEW ${DB}.v_rmv AS SELECT id FROM rmv;
CREATE VIEW ${DB}.v_dict AS SELECT val AS id FROM dict;
CREATE VIEW ${DB}.v_dictget AS SELECT dictGet('dict', 'val', id) AS id FROM tmp_src;
CREATE VIEW ${DB}.v_rt AS SELECT id FROM rt;
CREATE VIEW ${DB}.v_rdb AS SELECT id FROM ${RDB}.rsrc;
CREATE VIEW ${RDB}.v_in_rdb AS SELECT id FROM ${RDB}.rsrc;
CREATE VIEW ${DB}.v_definer SQL SECURITY DEFINER AS SELECT id FROM tmp_shadow;
CREATE VIEW ${DB}.v_none SQL SECURITY NONE AS SELECT id FROM tmp_shadow;
CREATE TABLE ${DB}.t_check (x UInt64, CONSTRAINT c CHECK x <= (SELECT max(id) FROM ${DB}.v_shadow)) ENGINE = MergeTree ORDER BY x;
"

# Reload the views from their metadata in a session without the temporary tables.
for db in ${DB} ${RDB}; do
    $CLICKHOUSE_CLIENT --query "DETACH DATABASE ${db}"
    $CLICKHOUSE_CLIENT --query "ATTACH DATABASE ${db}"
done

# A new session reads its own temporary tables through the views.
for view in ${DB}.{v_from,v_join,v_in,v_scalar,v_shadow,v_tview,v_view,v_mv,v_rmv,v_dict,v_dictget,v_rt,v_rdb,v_definer,v_none} ${RDB}.v_in_rdb; do
    $CLICKHOUSE_CLIENT --query "
    CREATE TEMPORARY TABLE tmp_src (id UInt64);
    CREATE TEMPORARY TABLE tmp_shadow (id UInt64);
    CREATE TEMPORARY VIEW tmp_view AS SELECT id FROM tmp_src;
    INSERT INTO tmp_src VALUES (1);
    INSERT INTO tmp_shadow VALUES (1);
    SELECT '${view#*.}', groupArray(id) FROM ${view};
    " 2>&1 | grep -m1 -oE "^v_[a-z_]+\s\[[0-9,]*\]$|UNKNOWN_[A-Z]+"
done

# Without a temporary table of that name the view reads the table of its database,
# also when a constraint of another table reads the view while that table is attached.
$CLICKHOUSE_CLIENT --query "CREATE TEMPORARY TABLE tmp_shadow (id UInt64); ALTER TABLE ${DB}.v_shadow MODIFY COMMENT 'c'"
$CLICKHOUSE_CLIENT --query "SELECT 'v_shadow', groupArray(id) FROM ${DB}.v_shadow"
$CLICKHOUSE_CLIENT --query "SELECT 't_check', count() FROM ${DB}.t_check" 2>&1 | grep -m1 -oE "^t_check\s[0-9]+$|[A-Z_]{12,}"

$CLICKHOUSE_CLIENT --query "DROP DATABASE ${DB}"
$CLICKHOUSE_CLIENT --query "DROP DATABASE ${RDB}"
