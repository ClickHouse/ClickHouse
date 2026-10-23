#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES_UNIQUE}_prewhere_subcolumns"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"
mkdir -p "${ROOT}"

$CLICKHOUSE_LOCAL --query "
INSERT INTO FUNCTION file('${ROOT}/data.parquet') SELECT number AS id, range(number % 5) AS arr, if(number % 2, NULL, number) AS n, tuple(number, toString(number)) :: Tuple(a UInt64, b String) AS t FROM numbers(100);

SELECT 'parquet prewhere size0', arr.size0 FROM file('${ROOT}/data.parquet') PREWHERE id = 13;
SELECT 'parquet prewhere null', n.null FROM file('${ROOT}/data.parquet') PREWHERE id = 13;
SELECT 'parquet prewhere column and size0', arr, arr.size0 FROM file('${ROOT}/data.parquet') PREWHERE id = 14;
SELECT 'parquet prewhere tuple element', t.b, arr.size0 FROM file('${ROOT}/data.parquet') PREWHERE id = 12;
SELECT 'parquet where size0', arr.size0, n.null FROM file('${ROOT}/data.parquet') WHERE id = 13;
SELECT 'parquet moved to prewhere', count() FROM (EXPLAIN actions = 1 SELECT arr.size0 FROM file('${ROOT}/data.parquet') WHERE id = 13) WHERE explain LIKE '%Prewhere filter column%';
"

$CLICKHOUSE_LOCAL --allow_delta_lake_writes=1 --allow_delta_lake_create_table=1 --query "
CREATE TABLE t_delta (id Nullable(Int64), arr Array(Int64), n Nullable(Int64)) ENGINE = DeltaLakeLocal('${ROOT}/delta', Parquet);
INSERT INTO t_delta SELECT number, range(number % 5), if(number % 2, NULL, number) FROM numbers(100);

SELECT 'delta prewhere size0', arr.size0 FROM t_delta PREWHERE id = 13;
SELECT 'delta prewhere null', n.null FROM t_delta PREWHERE id = 13;
SELECT 'delta where size0', arr.size0, n.null FROM t_delta WHERE id = 13;
SELECT 'delta moved to prewhere', count() FROM (EXPLAIN actions = 1 SELECT arr.size0 FROM t_delta WHERE id = 13) WHERE explain LIKE '%Prewhere filter column%';
"

SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"email\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'
mkdir -p "${ROOT}/cdf/_delta_log"
cat > "${ROOT}/cdf/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["changeDataFeed"]}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-cdf","format":{"provider":"parquet","options":{}},"schemaString":"${SCHEMA}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1700000000000}}
EOF

$CLICKHOUSE_LOCAL --allow_delta_lake_writes=1 --query "
INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/cdf') SELECT number AS id, concat('u', toString(number), '@x') AS email FROM numbers(20);

SET delta_lake_snapshot_start_version = 1;
SELECT 'cdf where', count(), min(id), max(id) FROM deltaLakeLocal('${ROOT}/cdf') WHERE length(email) = 4;
SELECT 'cdf where with output', id, email, _change_type FROM deltaLakeLocal('${ROOT}/cdf') WHERE length(email) = 5 ORDER BY id LIMIT 2;
SELECT 'cdf not moved to prewhere', count() FROM (EXPLAIN actions = 1 SELECT id FROM deltaLakeLocal('${ROOT}/cdf') WHERE length(email) = 4) WHERE explain LIKE '%Prewhere%';
SELECT id FROM deltaLakeLocal('${ROOT}/cdf') PREWHERE length(email) = 4; -- { serverError ILLEGAL_PREWHERE }
"
