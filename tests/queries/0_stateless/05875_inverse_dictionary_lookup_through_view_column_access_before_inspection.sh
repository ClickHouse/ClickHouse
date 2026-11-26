#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: Dictionary is not available on parallel-replica workers.

# The inverse dictionary lookup rewrite through a view must check that the invoker may read the view column
# before it looks into the view: inspecting the definition or loading its dictionary can fail, and that
# error must not be seen by a user who may not read the column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ref_table="ref_${CLICKHOUSE_TEST_UNIQUE_NAME}"
data_table="data_${CLICKHOUSE_TEST_UNIQUE_NAME}"
dict_name="dict_${CLICKHOUSE_TEST_UNIQUE_NAME}"
view_name="view_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db_name="${CLICKHOUSE_DATABASE}"

# The dictionary is not loaded yet, and its source is dropped, so that loading it fails.
${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${user};
    CREATE TABLE ${ref_table} (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${ref_table} VALUES (1, 'match'), (2, 'other');

    CREATE DICTIONARY ${dict_name} (id UInt64, name String)
    PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE '${ref_table}')) LAYOUT(HASHED_ARRAY()) LIFETIME(0);

    CREATE TABLE ${data_table} (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${data_table} VALUES (1), (2);

    CREATE VIEW ${view_name} SQL SECURITY INVOKER AS
    SELECT id, dictGetString('${db_name}.${dict_name}', 'name', id) AS name FROM ${data_table};

    SET check_table_dependencies = 0;
    DROP TABLE ${ref_table};

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT(id) ON ${db_name}.${view_name} TO ${user};
    GRANT SELECT ON ${db_name}.${data_table} TO ${user};
    GRANT dictGet ON ${db_name}.${dict_name} TO ${user};
"

# The user may use the dictionary, but not read `name`: the query is denied, with and without the optimization,
# and not with the error of loading the dictionary.
for setting in 0 1; do
    ${CLICKHOUSE_CLIENT} --user="${user}" --query "
        SELECT count() FROM ${db_name}.${view_name} WHERE name = 'other'
        SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = ${setting};
    " 2>&1 | grep -o -m1 'ACCESS_DENIED'
done

# Nothing has tried to load the dictionary.
${CLICKHOUSE_CLIENT} --query "
    SELECT status FROM system.dictionaries WHERE database = '${db_name}' AND name = '${dict_name}'
"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${user};
    DROP VIEW ${view_name};
    DROP DICTIONARY ${dict_name};
    DROP TABLE ${data_table};
"
