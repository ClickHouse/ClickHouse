#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: needs Iceberg and S3 (MinIO)
# Tag no-replicated-database: DETACH TABLE is not allowed in a Replicated database

# A named collection override given as an expression keeps the value it had at CREATE
# after DETACH/ATTACH and after a restart, as a positional engine argument does.
# https://github.com/ClickHouse/ClickHouse/issues/126507

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="toString(toUnixTimestamp64Nano(now64(9)))"
NC="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
P="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_"
L="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"

# The tables are dropped before the collections they use, and the collections are kept if anything fails. A client killed between
# DETACH and ATTACH leaves a table detached, which DROP TABLE IF EXISTS skips, so it is attached back first.
cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "
        SET ast_fuzzer_any_query = 0;
        ATTACH TABLE IF NOT EXISTS t_kv; ATTACH TABLE IF NOT EXISTS t_pos; ATTACH TABLE IF NOT EXISTS t_lit; ATTACH TABLE IF NOT EXISTS t_s3;
        DROP TABLE IF EXISTS t_kv; DROP TABLE IF EXISTS t_pos; DROP TABLE IF EXISTS t_lit; DROP TABLE IF EXISTS t_s3;
    " && ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${NC}; DROP NAMED COLLECTION IF EXISTS ${NC}_s3"
    rm -rf "${P}"* "${L:?}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "
CREATE NAMED COLLECTION ${NC} AS format = 'Parquet';
CREATE NAMED COLLECTION ${NC}_s3 AS url = 'http://localhost:11111/test/', access_key_id = 'test', secret_access_key = 'testtest', format = 'CSV';
"

# ast_fuzzer_any_query = 0: the AST fuzzer must not replay these DDLs as clones that outlive the collections.
${CLICKHOUSE_CLIENT} -q "
SET ast_fuzzer_any_query = 0;
CREATE TABLE t_kv (x Int32) ENGINE = IcebergLocal(${NC}, path = concat('${P}kv_', ${T}));
CREATE TABLE t_pos (x Int32) ENGINE = IcebergLocal(concat('${P}pos_', ${T}));
CREATE TABLE t_lit (x Int32) ENGINE = IcebergLocal(${NC}, path = '${P}lit');
CREATE TABLE t_s3 (x Int32) ENGINE = S3(${NC}_s3, filename = concat('${CLICKHOUSE_TEST_UNIQUE_NAME}_', ${T}, '.csv'));
"

${CLICKHOUSE_CLIENT} -q "
INSERT INTO t_kv SETTINGS allow_insert_into_iceberg = 1 VALUES (1);
INSERT INTO t_pos SETTINGS allow_insert_into_iceberg = 1 VALUES (1);
INSERT INTO t_lit SETTINGS allow_insert_into_iceberg = 1 VALUES (1);
INSERT INTO t_s3 SETTINGS s3_truncate_on_insert = 1 VALUES (1);
"

for t in t_kv t_pos t_lit t_s3; do
    ${CLICKHOUSE_CLIENT} -q "SET ast_fuzzer_any_query = 0; DETACH TABLE ${t}"
    ${CLICKHOUSE_CLIENT} -q "SET ast_fuzzer_any_query = 0; ATTACH TABLE ${t}"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${t}"
done

# The stored definition holds the value, not the expression.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_kv' AND engine_full LIKE '%now64%'"

# Restart: a second clickhouse-local process with the same --path. It reads local lake tables under its working directory.
mkdir -p "${L}"
(
    cd "${L}" || exit 1
    ${CLICKHOUSE_LOCAL} --path db -q "
        SET allow_insert_into_iceberg = 1;
        CREATE NAMED COLLECTION nc AS format = 'Parquet';
        CREATE DATABASE d ENGINE = Atomic;
        CREATE TABLE d.t (x Int32) ENGINE = IcebergLocal(nc, path = concat('kv_', ${T}));
        INSERT INTO d.t VALUES (1);
    "
    ${CLICKHOUSE_LOCAL} --path db -q "SELECT count() FROM d.t"
)
