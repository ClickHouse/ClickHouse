#!/usr/bin/env bash

# `ALTER UPDATE`, `UPDATE ... SET` and `ALTER MODIFY TTL` evaluate expressions over the table data and write the
# result into columns (or delete rows by it), so they require `SELECT` on every column those expressions read.
# Otherwise `UPDATE public = secret` copies a column the user cannot read into one they can.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05830_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;
CREATE TABLE $db.records (id UInt64, d Date, public String, secret String, t Tuple(a String, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO $db.records VALUES (1, '2100-01-01', 'public_1', 'secret_1', ('a_1', 'b_1')), (2, '2100-01-01', 'public_2', 'secret_2', ('a_2', 'b_2'));
-- `UPDATE ... SET` requires `ALTER UPDATE` on the whole table, so grant it there: only `SELECT` can be missing below.
GRANT SELECT(id, public), ALTER UPDATE, ALTER TTL ON $db.records TO $user;
EOF

echo "-- denied: UPDATE reading secret"
${CLICKHOUSE_CLIENT} --user "$user" --mutations_sync 2 <<EOF
ALTER TABLE $db.records UPDATE public = secret WHERE 1; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records UPDATE public = concat(public, secret) WHERE id = 1; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records UPDATE public = 'x' WHERE secret = 'secret_1'; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records UPDATE public = t.a WHERE 1; -- { serverError ACCESS_DENIED }
UPDATE $db.records SET public = secret WHERE 1 SETTINGS enable_lightweight_update = 1; -- { serverError ACCESS_DENIED }
UPDATE $db.records SET public = 'x' WHERE startsWith(secret, 'secret_1') SETTINGS enable_lightweight_update = 1; -- { serverError ACCESS_DENIED }
EOF

echo "-- denied: TTL reading columns without SELECT"
${CLICKHOUSE_CLIENT} --user "$user" <<EOF
ALTER TABLE $db.records MODIFY TTL d + INTERVAL 1 DAY; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records MODIFY TTL toDate('2100-01-01') + INTERVAL 1 DAY WHERE secret = 'secret_1'; -- { serverError ACCESS_DENIED }
ALTER TABLE $db.records MODIFY TTL toDate('2100-01-01') + INTERVAL 1 DAY GROUP BY id SET public = any(secret); -- { serverError ACCESS_DENIED }
EOF

echo "-- data and metadata unchanged"
${CLICKHOUSE_CLIENT} --query "SELECT id, public FROM $db.records ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = '$db' AND table = 'records'"
${CLICKHOUSE_CLIENT} --query "SELECT position(create_table_query, 'TTL') > 0 FROM system.tables WHERE database = '$db' AND name = 'records'"

echo "-- allowed: expressions reading only granted columns"
${CLICKHOUSE_CLIENT} --user "$user" --mutations_sync 2 <<EOF
ALTER TABLE $db.records UPDATE public = upper(public) WHERE id = 1;
UPDATE $db.records SET public = concat(public, '_', toString(id)) WHERE id = 2 SETTINGS enable_lightweight_update = 1;
ALTER TABLE $db.records MODIFY TTL toDate('2100-01-01') + INTERVAL 1 DAY GROUP BY id SET public = any(public);
SELECT id, public FROM $db.records ORDER BY id;
EOF

echo "-- allowed: a grant on the column covers its subcolumns"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(t) ON $db.records TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --mutations_sync 2 <<EOF
ALTER TABLE $db.records UPDATE public = t.a WHERE id = 1;
SELECT id, public FROM $db.records ORDER BY id;
EOF

echo "-- allowed after GRANT SELECT(secret, d)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(secret, d) ON $db.records TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --mutations_sync 2 <<EOF
ALTER TABLE $db.records UPDATE public = secret WHERE id = 1;
UPDATE $db.records SET public = secret WHERE id = 2 SETTINGS enable_lightweight_update = 1;
ALTER TABLE $db.records MODIFY TTL d + INTERVAL 1 DAY WHERE secret = 'secret_1';
SELECT id, public FROM $db.records ORDER BY id;
EOF

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
DROP TABLE IF EXISTS $db.records;
EOF
