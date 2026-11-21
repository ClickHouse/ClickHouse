#!/usr/bin/env bash

# Storing a column expression (`DEFAULT`, `MATERIALIZED`, `ALIAS`, `EPHEMERAL`, `TTL`) with `ALTER ADD/MODIFY COLUMN`
# requires `SELECT` on every column the expression reads, otherwise `public ALIAS secret` exposes `secret`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05829_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;
CREATE TABLE $db.records (id UInt64, d Date, public String, secret String, t Tuple(a String, b String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.records VALUES (1, '2100-01-01', 'public_1', 'secret_1', ('a_1', 'b_1')), (2, '2100-01-01', 'public_2', 'secret_2', ('a_2', 'b_2'));
GRANT SELECT(id, public), ALTER MODIFY COLUMN(public), ALTER ADD COLUMN ON $db.records TO $user;
EOF

echo "-- denied: expressions reading secret"
${CLICKHOUSE_CLIENT} --user "$user" <<EOF
ALTER TABLE $db.records MODIFY COLUMN public String ALIAS secret; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records MODIFY COLUMN public String ALIAS concat(public, secret); -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records MODIFY COLUMN public String DEFAULT secret; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records MODIFY COLUMN public String MATERIALIZED upper(secret); -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records MODIFY COLUMN public String TTL d + INTERVAL 1 DAY; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records ADD COLUMN c1 String ALIAS secret; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records ADD COLUMN c2 String DEFAULT arrayStringConcat(arrayMap(x -> concat(x, secret), [public])); -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records ADD COLUMN c3 String ALIAS t.a; -- { serverError ACCESS_DENIED }
SELECT groupArray(public) FROM $db.records;
EOF

echo "-- metadata unchanged"
${CLICKHOUSE_CLIENT} --query "SELECT name, default_kind, default_expression FROM system.columns WHERE database = '$db' AND table = 'records' ORDER BY position"

echo "-- allowed: expressions reading only granted columns, lambda parameters are not columns"
${CLICKHOUSE_CLIENT} --user "$user" <<EOF
ALTER TABLE $db.records ADD COLUMN c4 String ALIAS arrayStringConcat(arrayMap(x -> upper(x), [public]));
ALTER TABLE $db.records ADD COLUMN c5 String DEFAULT concat(public, toString(id));
EOF

echo "-- allowed: a grant on the column covers its subcolumns"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(t) ON $db.records TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "ALTER TABLE $db.records ADD COLUMN c6 String ALIAS t.a"

echo "-- allowed after GRANT SELECT(secret)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(secret) ON $db.records TO $user"
${CLICKHOUSE_CLIENT} --user "$user" <<EOF
ALTER TABLE $db.records MODIFY COLUMN public String ALIAS secret;
SELECT groupArray(public) FROM $db.records;
EOF

${CLICKHOUSE_CLIENT} --query "SELECT name, default_kind, default_expression FROM system.columns WHERE database = '$db' AND table = 'records' ORDER BY position"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
DROP TABLE IF EXISTS $db.records;
EOF
