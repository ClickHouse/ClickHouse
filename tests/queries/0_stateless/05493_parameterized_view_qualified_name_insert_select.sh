#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: the test creates a Replicated database itself.

# Older versions stored a call `db.view(...)` of a parameterized view as `` `db.view`(...) ``.
# INSERT ... SELECT into a replicated or distributed table, a refreshable materialized view
# reloaded from such metadata or defined in a Replicated database, and EXPLAIN SYNTAX must
# resolve that spelling to the view `view` of database `db`, as SELECT does.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

a="${CLICKHOUSE_DATABASE}_a"
r="${CLICKHOUSE_DATABASE}_r"

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${a} ENGINE = Atomic;
    CREATE TABLE ${a}.src (d DateTime, x UInt8) ENGINE = MergeTree ORDER BY d;
    INSERT INTO ${a}.src SELECT toDateTime(number), 1 + (number >= 3) FROM numbers(5);
    CREATE VIEW ${a}.pv AS SELECT d, x FROM ${a}.src WHERE x = {p:UInt8};
"

# Only a non-zero parallel_distributed_insert_select sends the INSERT through the checks that failed.
# Parallel replicas are off: every replica of the test cluster would read the whole view, so the count would be wrong.
echo 'into ReplicatedMergeTree'
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${a}.t_repl (d DateTime, x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_repl', '1') ORDER BY d;
    INSERT INTO ${a}.t_repl SELECT * FROM \`${a}.pv\`(p = 1) SETTINGS parallel_distributed_insert_select = 2, enable_parallel_replicas = 0;
    SELECT count() FROM ${a}.t_repl;
"

echo 'into Distributed'
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${a}.t_local (d DateTime, x UInt8) ENGINE = MergeTree ORDER BY d;
    CREATE TABLE ${a}.dist AS ${a}.t_local ENGINE = Distributed(test_shard_localhost, ${a}, t_local);
    INSERT INTO ${a}.dist SELECT * FROM \`${a}.pv\`(p = 1) SETTINGS parallel_distributed_insert_select = 2, enable_parallel_replicas = 0, distributed_foreground_insert = 1;
    SELECT count() FROM ${a}.t_local;
"

# DETACH/ATTACH reads the view's definition back from its metadata file, as a server restart does.
# The view may refresh on ATTACH as well, so the checks do not depend on the number of refreshes.
echo 'refreshable view reloaded from metadata, feeding a materialized view'
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${a}.t_rmv (d DateTime, x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmv', '1') ORDER BY d;
    CREATE TABLE ${a}.sink (d DateTime, x UInt8) ENGINE = MergeTree ORDER BY d;
    CREATE MATERIALIZED VIEW ${a}.mv_sink TO ${a}.sink AS SELECT d, x FROM ${a}.t_rmv;
    CREATE MATERIALIZED VIEW ${a}.rmv REFRESH EVERY 1 YEAR APPEND TO ${a}.t_rmv EMPTY AS SELECT * FROM \`${a}.pv\`(p = 1);
    DETACH TABLE ${a}.rmv;
    ATTACH TABLE ${a}.rmv;
    SYSTEM REFRESH VIEW ${a}.rmv;
    SYSTEM WAIT VIEW ${a}.rmv;
    SELECT uniqExact(d), count() > 0 FROM ${a}.t_rmv;
    SELECT uniqExact(d), count() = (SELECT count() FROM ${a}.t_rmv) FROM ${a}.sink;
"

echo 'refreshable view in a Replicated database'
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE DATABASE ${r} ENGINE = Replicated('/test/{database}/parameterized_view_qualified_name', 's1', 'r1');
    CREATE TABLE ${r}.src (d DateTime, x UInt8) ENGINE = MergeTree ORDER BY d;
    INSERT INTO ${r}.src SELECT toDateTime(number), 1 + (number >= 3) FROM numbers(5);
    CREATE VIEW ${r}.pv AS SELECT d, x FROM ${r}.src WHERE x = {p:UInt8};
    CREATE TABLE ${r}.t_repl (d DateTime, x UInt8) ENGINE = ReplicatedMergeTree ORDER BY d;
    CREATE MATERIALIZED VIEW ${r}.rmv REFRESH EVERY 1 YEAR APPEND TO ${r}.t_repl EMPTY AS SELECT * FROM \`${r}.pv\`(p = 1);
    SYSTEM REFRESH VIEW ${r}.rmv;
    SYSTEM WAIT VIEW ${r}.rmv;
    SELECT count() FROM ${r}.t_repl;
"

echo 'EXPLAIN SYNTAX inlines the view'
${CLICKHOUSE_CLIENT} -q "SELECT countIf(explain LIKE '%FROM ${a}.src%') > 0 FROM (EXPLAIN SYNTAX SELECT * FROM \`${a}.pv\`(p = 1))"

echo 'the formatted query keeps the two parts of the name'
${CLICKHOUSE_CLIENT} -q "
    SELECT formatQuerySingleLine('SELECT * FROM db.pv(p = 1)');
    SELECT formatQuerySingleLine('SELECT * FROM \`my-db\`.pv(p = 1)');
    SELECT formatQuerySingleLine('SELECT * FROM \`db.pv\`(p = 1)');
"

echo 'a keyword part stays quoted and parses back'
${CLICKHOUSE_CLIENT} -q "
    SELECT formatQuerySingleLine(q) AS f, formatQuerySingleLine(f) = f
    FROM (SELECT arrayJoin([
        'SELECT * FROM \`null\`.pv(p = 1)', 'SELECT 1 IN \`null\`.pv(1)',
        'SELECT * FROM \`not\`.pv(p = 1)', 'SELECT 1 IN \`not\`.pv(1)',
        'SELECT 1 IN \`partition\`.pv(1)']) AS q);
"

# This is the query the analyzer sends to other servers, for example for a subquery in IN.
echo 'the analyzer writes the stored spelling as two parts'
${CLICKHOUSE_CLIENT} -q "SELECT countIf(explain LIKE '%FROM ${a}.pv(p = 1)%') FROM (EXPLAIN QUERY TREE dump_ast = 1 SELECT * FROM \`${a}.pv\`(p = 1))"

echo 'a view whose own name has a dot'
${CLICKHOUSE_CLIENT} -q "
    CREATE VIEW \`v.x\` AS SELECT d FROM ${a}.src WHERE x = {p:UInt8};
    SELECT countIf(explain LIKE '%FROM ${a}.src%') > 0 FROM (EXPLAIN SYNTAX SELECT * FROM \`v.x\`(p = 1));
"

echo 'a view whose own name has a dot, when the split names a table that is not a parameterized view'
${CLICKHOUSE_CLIENT} -q "
    CREATE VIEW \`${a}.src\` AS SELECT d FROM ${a}.src WHERE x = {p:UInt8};
    SELECT countIf(explain LIKE '%FROM ${a}.src%') > 0 FROM (EXPLAIN SYNTAX SELECT * FROM \`${a}.src\`(p = 1));
"

# Names are hierarchical (see 05077_hierarchical_names): without a database `v`, `v.x` is the view `v.x` of the current database.
echo 'an unquoted v.x(...) without a database v is the view v.x'
${CLICKHOUSE_CLIENT} -q "EXPLAIN SYNTAX SELECT * FROM v.x(p = 1)" | grep -c "FROM ${a}.src"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE ${r} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${a} SYNC"
