#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and Avro are not in fast test

# Iceberg tables written by Trino, see `data_iceberg/trino/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/trino"

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`.
cd "$DATA" || exit 1

$CLICKHOUSE_LOCAL -q "
-- Avro, Parquet and ORC data files of tables whose \`write.*.compression-codec\` property is invalid
-- for the format (\`NONE\` for Avro and Parquet, \`GZIP\` for ORC), written by Trino 477
-- (https://github.com/trinodb/trino/issues/28293). The expected results come from the Trino test
-- \`TestIcebergInvalidCompressionCodecs\`.
SELECT 'invalid_compression_codec';
DESCRIBE icebergLocal('invalid_compression_codec/none_avro');
SELECT * FROM icebergLocal('invalid_compression_codec/none_avro');
DESCRIBE icebergLocal('invalid_compression_codec/none_parquet');
SELECT * FROM icebergLocal('invalid_compression_codec/none_parquet');
DESCRIBE icebergLocal('invalid_compression_codec/orc_gzip');
SELECT * FROM icebergLocal('invalid_compression_codec/orc_gzip');

-- An empty table with a mixed-case column name (\`dataColumn\`) and a Parquet bloom filter
-- property for it, from the Trino test \`TestIcebergParquetWithBloomFiltersMixedCase\`.
SELECT 'mixed_case_bloom_filter';
DESCRIBE icebergLocal('mixed_case_bloom_filter');
SELECT count() FROM icebergLocal('mixed_case_bloom_filter');
SELECT count() FROM icebergLocal('mixed_case_bloom_filter') WHERE dataColumn = 1;
"
