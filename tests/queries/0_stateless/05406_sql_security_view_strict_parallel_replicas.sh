#!/usr/bin/env bash

# The query of a `SQL SECURITY DEFINER` / `NONE` view is read locally, not with parallel replicas,
# so what parallel replicas do not support must not make it fail even with `enable_parallel_replicas = 2`,
# including when the definer's own profile or the `SETTINGS` clause of the view's query turns them on.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

definer="definer05406_${CLICKHOUSE_DATABASE}_$RANDOM"
profile="profile05406_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

pr_settings="enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 0, parallel_replicas_mode = 'read_tasks', automatic_parallel_replicas_mode = 0"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $definer;
DROP SETTINGS PROFILE IF EXISTS $profile;
CREATE USER $definer;
CREATE SETTINGS PROFILE $profile SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'k' TO $definer;

CREATE TABLE $db.t (k UInt64, v String) ENGINE = ReplacingMergeTree ORDER BY k;
INSERT INTO $db.t VALUES (1, 'a'), (2, 'b');
INSERT INTO $db.t VALUES (1, 'c');
GRANT SELECT ON $db.t TO $definer;

CREATE VIEW $db.final_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, v FROM $db.t FINAL;
CREATE VIEW $db.none_final_view SQL SECURITY NONE AS SELECT k, v FROM $db.t FINAL;
CREATE VIEW $db.join_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT a.k AS k, b.v AS v FROM $db.t AS a INNER JOIN $db.t AS b ON a.k = b.k AND a.v = b.v;
CREATE VIEW $db.settings_final_view (k UInt64, v String) SQL SECURITY NONE AS SELECT k, v FROM $db.t FINAL SETTINGS $pr_settings;
CREATE VIEW $db.settings_join_view (k UInt64, v String) DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT a.k AS k, b.v AS v FROM $db.t AS a INNER JOIN $db.t AS b ON a.k = b.k AND a.v = b.v SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'k';
CREATE VIEW $db.subquery_settings_final_view (k UInt64, v String) SQL SECURITY NONE AS SELECT k, v FROM (SELECT k, v FROM $db.t FINAL SETTINGS $pr_settings);
EOF

echo "--- FINAL in the query of the view"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $db.final_view ORDER BY k SETTINGS $pr_settings"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $db.none_final_view ORDER BY k SETTINGS $pr_settings"

echo "--- JOIN in the query of the view, whose definer enables custom-key parallel replicas"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $db.join_view ORDER BY k, v"

echo "--- parallel replicas turned on by the SETTINGS clause of the query of the view"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $db.settings_final_view ORDER BY k"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $db.settings_join_view ORDER BY k, v"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $db.subquery_settings_final_view ORDER BY k"

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.join_view; DROP USER $definer; DROP SETTINGS PROFILE $profile"
