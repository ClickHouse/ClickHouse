#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Protobuf is not built in the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `format_schema_source = 'query'` the schema is read by a query with its own pipeline executor, which
# accepts only a `String` column. A schema stored in a column with automatic `LowCardinality` serialization
# keeps the declared type `String`, so the encoding must be materialized and the schema must be accepted.

PROTO_SCHEMA='syntax = "proto3"; message KVP { uint64 key = 1; string value = 2; }'

${CLICKHOUSE_LOCAL} --logger.console=0 --query "
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

CREATE TABLE schemas (id UInt64, s String STATISTICS(uniq)) ENGINE = MergeTree ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 100, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
INSERT INTO schemas VALUES (1, '${PROTO_SCHEMA}');

-- The output is the Protobuf stream, so the storage of the schema is asserted instead of printed.
SELECT throwIf(countIf(serialization_kind = 'LowCardinality') != 1) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'schemas' AND column = 's' AND active FORMAT Null;

SELECT 1 AS key, 'abc' AS value
SETTINGS format_schema_source = 'query', format_schema = 'SELECT s FROM schemas LIMIT 1', format_schema_message_name = 'KVP'
FORMAT Protobuf;
" | ${CLICKHOUSE_LOCAL} --logger.console=0 --input-format Protobuf --structure 'key UInt64, value String' --query "
SELECT * FROM table
SETTINGS
    format_schema_source = 'query',
    format_schema = 'SELECT ''${PROTO_SCHEMA}''',
    format_schema_message_name = 'KVP';
" 2>&1
