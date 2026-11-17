#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user with `SELECT` only on a `Merge` wrapper must not learn the row count or size of a matched
# table they have no privilege on, neither from the trivial count nor from `system.tables`.

db="${CLICKHOUSE_DATABASE}"
scratch="${CLICKHOUSE_DATABASE}_scratch"
user="low_probe_${CLICKHOUSE_DATABASE}"
# The settings under which `count()` is answered from metadata (trivial count).
trivial="optimize_trivial_count_query = 1, apply_patch_parts = 0, apply_mutations_on_fly = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --multiline -q "
DROP DATABASE IF EXISTS ${scratch};
DROP USER IF EXISTS ${user};

CREATE TABLE allowed (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO allowed SELECT arrayJoin([10, 20]);

CREATE TABLE protected (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO protected SELECT arrayJoin([101, 102, 103]);

CREATE TABLE merge_wrapper (v UInt64) ENGINE = Merge('${db}', '^protected\$');
CREATE TABLE excluded_wrapper (v UInt64) ENGINE = Merge('${db}', '^does_not_match\$');

CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT SELECT ON ${db}.allowed TO ${user};
GRANT SELECT ON ${db}.merge_wrapper TO ${user};
GRANT SELECT ON ${db}.excluded_wrapper TO ${user};

CREATE DATABASE ${scratch};
GRANT SELECT, CREATE TABLE ON ${scratch}.* TO ${user};
GRANT TABLE ENGINE ON Merge TO ${user};
"

${CLICKHOUSE_CLIENT} --user "${user}" --multiline -q "
SELECT count() FROM protected SETTINGS ${trivial}; -- { serverError ACCESS_DENIED }
SELECT 'allowed', count() FROM allowed SETTINGS ${trivial};

SELECT 'merge_wrapper rows', v FROM merge_wrapper ORDER BY v SETTINGS apply_patch_parts = 0;
SELECT 'merge_wrapper count', count() FROM merge_wrapper SETTINGS optimize_trivial_count_query = 0, apply_patch_parts = 0;
SELECT 'merge_wrapper trivial count', count() FROM merge_wrapper SETTINGS ${trivial};
SELECT 'excluded_wrapper trivial count', count() FROM excluded_wrapper SETTINGS ${trivial};

SELECT 'system.tables', name, total_rows FROM system.tables WHERE database = '${db}' ORDER BY name;
SELECT 'system.tables', name, total_bytes FROM system.tables WHERE database = '${db}' AND name LIKE '%wrapper' ORDER BY name;

-- The same through a wrapper the user creates themselves.
CREATE TABLE ${scratch}.mywrap (v UInt64) ENGINE = Merge('${db}', '^protected\$');
SELECT 'system.tables', name, total_rows, total_bytes FROM system.tables WHERE database = '${scratch}' AND name = 'mywrap';
SELECT 'mywrap count', count() FROM ${scratch}.mywrap;
SELECT 'mywrap trivial count', count() FROM ${scratch}.mywrap SETTINGS ${trivial};
"

# Once the protected table is visible but still not readable, every count is denied.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES, SHOW COLUMNS ON ${db}.protected TO ${user}"

${CLICKHOUSE_CLIENT} --user "${user}" --multiline -q "
SELECT v FROM merge_wrapper ORDER BY v SETTINGS apply_patch_parts = 0; -- { serverError ACCESS_DENIED }
SELECT count() FROM merge_wrapper SETTINGS optimize_trivial_count_query = 0, apply_patch_parts = 0; -- { serverError ACCESS_DENIED }
SELECT count() FROM merge_wrapper SETTINGS ${trivial}; -- { serverError ACCESS_DENIED }
SELECT count() FROM ${scratch}.mywrap SETTINGS ${trivial}; -- { serverError ACCESS_DENIED }
"

${CLICKHOUSE_CLIENT} --multiline -q "
SELECT 'admin merge_wrapper trivial count', count() FROM merge_wrapper SETTINGS ${trivial};
SELECT 'admin system.tables', name, total_rows FROM system.tables WHERE database = '${db}' AND name = 'merge_wrapper';
DROP DATABASE ${scratch};
DROP USER ${user};
"
