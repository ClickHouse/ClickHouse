#!/usr/bin/env bash
# DROP and RENAME COLUMN analyze a SQL SECURITY DEFINER view as its definer: the caller needs no grant on the tables it reads.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05628_${CLICKHOUSE_DATABASE}"
definer="definer_05628_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user}, ${definer};
    CREATE TABLE src (k Int32, b Int32, c Int32, d Int32) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE lookup (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE dst (b Int32, v Int32) ENGINE = MergeTree ORDER BY b;
    CREATE MATERIALIZED VIEW mv TO dst DEFINER = CURRENT_USER SQL SECURITY DEFINER
        AS SELECT s.b AS b, l.v AS v FROM src AS s LEFT JOIN lookup AS l ON s.k = l.k;
    CREATE USER ${user};
    GRANT ALTER RENAME COLUMN, ALTER DROP COLUMN ON ${CLICKHOUSE_DATABASE}.src TO ${user};"

${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE src RENAME COLUMN c TO c2"
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE src DROP COLUMN d"
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE src RENAME COLUMN b TO b2" 2>&1 | grep -o -m1 'ALTER_OF_COLUMN_IS_FORBIDDEN'

# A definer without the grant on lookup cannot analyze its view.
${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${definer};
    CREATE MATERIALIZED VIEW mv2 TO dst DEFINER = ${definer} SQL SECURITY DEFINER
        AS SELECT s.b AS b, l.v AS v FROM src AS s LEFT JOIN lookup AS l ON s.k = l.k;"
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE src RENAME COLUMN c2 TO c3" 2>&1 | grep -o -m1 'ACCESS_DENIED'

${CLICKHOUSE_CLIENT} --query "
    SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'src' ORDER BY position);
    DROP TABLE mv2;
    DROP USER ${user}, ${definer};"
