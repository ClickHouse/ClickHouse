#!/usr/bin/env bash

# The columns of an `ABR` table, with their definitions and sizes, are those of its finest source table. `DESCRIBE` and
# `system.columns` show them only to a user allowed to see the columns of that table, not to one who can only read the
# `ABR` table, also for a column added to the source tables after the `ABR` table was created.
# The source tables have wide parts, in which the sizes of each column are known.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_experimental_abr=1 --query "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS abr_proxy;
    DROP TABLE IF EXISTS src_1;
    DROP TABLE IF EXISTS src_10;

    CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO src_1 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number, number FROM numbers(1000);
    INSERT INTO src_10 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number, number FROM numbers(1000);

    CREATE TABLE abr_proxy ENGINE = ABR('src', [1, 10], ('ts'));
    ALTER TABLE src_1 ADD COLUMN note String DEFAULT 'xyzzy';
    ALTER TABLE src_10 ADD COLUMN note String DEFAULT 'xyzzy';

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON abr_proxy TO ${user};
"

query="
    SELECT name, default_expression, data_compressed_bytes > 0, data_uncompressed_bytes > 0, marks_bytes > 0
    FROM system.columns WHERE database = currentDatabase() AND table = 'abr_proxy' ORDER BY position"

echo "user with access to the source tables"
${CLICKHOUSE_CLIENT} --query "${query}"

echo "user with access to the ABR table only"
${CLICKHOUSE_CLIENT} --user="${user}" --query "${query}"
${CLICKHOUSE_CLIENT} --user="${user}" --query "DESCRIBE TABLE abr_proxy" 2>&1 | grep -o -e "ACCESS_DENIED" -e "xyzzy" | sort -u

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON src_1 TO ${user}"
echo "user with access to the columns of the finest source table"
${CLICKHOUSE_CLIENT} --user="${user}" --query "${query}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE abr_proxy;
    DROP TABLE src_1;
    DROP TABLE src_10;
    DROP USER ${user};
"
