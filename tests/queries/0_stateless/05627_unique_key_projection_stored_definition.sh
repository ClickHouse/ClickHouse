#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage, no-s3-storage, no-fasttest, no-ordinary-database
# no-shared-merge-tree, no-object-storage, no-s3-storage: UNIQUE KEY needs a plain MergeTree on a disk with transactions.
# no-fasttest: UNIQUE KEY INSERT writes the dense-index SST, which needs RocksDB.
# no-ordinary-database: UNIQUE KEY tables are not supported in an Ordinary database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A unique-key table that also carries a projection must never read through the projection part (that would
# bypass the delete-bitmap filter). A new definition cannot have the combination, but a stored one still
# loads: the stored definition of a unique-key table is rewritten to carry a projection and attached back.

$CLICKHOUSE_CLIENT --enable_unique_key 1 -q "CREATE TABLE uk_t_attach_proj (id UInt64, user_id UInt32) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id, user_id)"
metadata_file=$($CLICKHOUSE_CLIENT -q "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'uk_t_attach_proj'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE uk_t_attach_proj"
perl -0pi -e 's/`user_id` UInt32\n\)/`user_id` UInt32,\n    PROJECTION p (SELECT user_id, count() GROUP BY user_id)\n)/' "$metadata_file"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE uk_t_attach_proj"

$CLICKHOUSE_CLIENT --enable_unique_key 1 -m <<'EOF'
-- Need parts for the projection optimizer to consider the projection.
INSERT INTO uk_t_attach_proj VALUES (1, 10), (2, 10), (3, 20);
-- Plain base-table reads still work: the projection is declined, not the read.
SELECT id, user_id FROM uk_t_attach_proj ORDER BY id;
-- An aggregating query the optimizer would route through the projection falls back to the base table.
SELECT user_id, count() FROM uk_t_attach_proj GROUP BY user_id ORDER BY user_id SETTINGS optimize_use_projections = 1;
-- Reading the projection part directly fails closed.
SELECT * FROM mergeTreeProjection(currentDatabase(), uk_t_attach_proj, p); -- { serverError NOT_IMPLEMENTED }
DROP TABLE uk_t_attach_proj SYNC;
EOF
