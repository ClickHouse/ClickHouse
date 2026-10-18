#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Paimon.
#
# Dropping a resolved table function forwards the drop to the storage behind it, but only an
# Iceberg storage may delete the data on drop. A Paimon storage keeps its files and does not know
# its row count, so `DROP TABLE ... IF EMPTY` of a resolved `paimonLocal` table function only
# removes the table from the catalog and goes ahead, as it does before the first read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/data_minio"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition/" "${DATA_DIR}/"
LAKE="${DATA_DIR}/paimon_no_partition"

FILES_BEFORE=$(find "${LAKE}" -type f | wc -l)

# ignore_drop_queries_probability = 0: the stress runner sets it, and a rewritten `DROP` becomes a
# `TRUNCATE`.
${CLICKHOUSE_CLIENT} --query "
    SET ignore_drop_queries_probability = 0;
    CREATE TABLE tf AS paimonLocal('${LAKE}');
    SELECT count() > 0 FROM tf;
    DROP TABLE IF EMPTY tf SYNC;
    SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tf';
"

FILES_AFTER=$(find "${LAKE}" -type f | wc -l)
[ "${FILES_BEFORE}" -eq "${FILES_AFTER}" ] && echo 'files preserved'

rm -rf "${DATA_DIR}"
