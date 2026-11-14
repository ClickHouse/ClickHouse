#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: needs the `S3Queue` engine
# Tag no-replicated-database: named collections are server-global

# A direct `SELECT` from an `S3Queue` created with `s3_allow_server_credentials_in_user_queries = 1`
# is checked against the reading session, not the creator's allowance kept for background processing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="s3queue_direct_${CLICKHOUSE_DATABASE}"
NC="s3queue_direct_nc_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SET ast_fuzzer_any_query = 0; DROP TABLE IF EXISTS ${TABLE} SYNC" && $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION IF NOT EXISTS ${NC} AS
        url = 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_direct/',
        use_environment_credentials = 1
"

$CLICKHOUSE_CLIENT -q "
    SET ast_fuzzer_any_query = 0;
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = S3Queue(${NC}, format = 'TSV')
    SETTINGS mode = 'ordered', s3_allow_server_credentials_in_user_queries = 1
"

$CLICKHOUSE_CLIENT --stream_like_engine_allow_direct_select 1 -q "SELECT * FROM ${TABLE}" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "not refused"
