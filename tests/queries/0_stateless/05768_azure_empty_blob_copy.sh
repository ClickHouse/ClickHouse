#!/usr/bin/env bash
# Tags: no-fasttest, no-distributed-cache, no-replicated-database
# Tag no-fasttest: requires Azure
# Tag no-distributed-cache: Not supported auth type
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

container="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"
db="${CLICKHOUSE_DATABASE}_azure"

# The flag of a permanently detached table is an empty file. Its removal copies the blob, which must be uploaded in a single part.
$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE ${db} ENGINE = Atomic SETTINGS disk = disk(
        name = '${container}',
        type = object_storage,
        metadata_type = plain_rewritable,
        object_storage_type = azure_blob_storage,
        endpoint = 'http://localhost:10000/devstoreaccount1/${container}/db',
        account_name = 'devstoreaccount1',
        account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
        use_native_copy = 0,
        max_single_part_upload_size = 0);
    CREATE TABLE ${db}.t (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${db}.t VALUES (1);
    DETACH TABLE ${db}.t PERMANENTLY;
    ATTACH TABLE ${db}.t;
    SELECT * FROM ${db}.t;
    DROP DATABASE ${db} SYNC;
"
