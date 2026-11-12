#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

# The columns of an `ABR` table in `system.columns` are hidden from a user who can only read the `ABR` table,
# also after re-attaching a database with `lazy_load_tables`, where the `ABR` table is still loaded eagerly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATABASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_db"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_experimental_abr=1 --query "
    DROP USER IF EXISTS ${user};
    DROP DATABASE IF EXISTS ${DATABASE};
    CREATE DATABASE ${DATABASE} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

    CREATE TABLE ${DATABASE}.src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE ${DATABASE}.src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO ${DATABASE}.src_1 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number, number FROM numbers(1000);
    INSERT INTO ${DATABASE}.src_10 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number, number FROM numbers(1000);

    CREATE TABLE ${DATABASE}.abr_proxy ENGINE = ABR('src', [1, 10], ('ts'));

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON ${DATABASE}.abr_proxy TO ${user};
"

${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DATABASE}"
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DATABASE}"

${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT name, data_compressed_bytes > 0, data_uncompressed_bytes > 0, marks_bytes > 0
    FROM system.columns WHERE database = '${DATABASE}' AND table = 'abr_proxy' ORDER BY position"

${CLICKHOUSE_CLIENT} --query "
    DROP DATABASE ${DATABASE};
    DROP USER ${user};
"
