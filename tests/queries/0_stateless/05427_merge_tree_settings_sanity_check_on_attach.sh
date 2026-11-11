#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table whose settings stop passing the sanity check (here, after the background pool was made smaller)
# is still loaded on ATTACH, and a warning about the settings is logged.

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t (x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 10;
    INSERT INTO d.t VALUES (1);
"

# With `background_pool_size = 2` there are only 4 tasks in the pool, which is less than 10.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --log-level=warning --logger.console=1 \
    --query "SELECT * FROM d.t" -- --background_pool_size=2 2>&1 \
    | grep -o -E '^1$|Table settings did not pass the sanity check' | sort | uniq

rm -rf "${WORKING_FOLDER}"
