#!/usr/bin/env bash
# The `is_lookup_index` flag of an index declaration can come from the `clickhouse_json` dialect. It must not
# exempt an ordinary data skipping index from the granularity check: a `minmax` index with zero granularity
# would fail on the first insert.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY='{"type":"CreateQuery","database":"","table":"t_lookup_json_flag","table_ast":{"type":"Identifier","name":"t_lookup_json_flag"},"columns_list":{"type":"Columns definition","columns":{"type":"ExpressionList","children":[{"type":"ColumnDeclaration","name":"id","data_type":{"type":"DataType","name":"UInt64"}}]},"indices":{"type":"ExpressionList","children":[{"type":"IndexDeclaration","name":"i","granularity":0,"is_lookup_index":true,"expression":{"type":"Identifier","name":"id"},"index_type":{"type":"Function","name":"minmax","no_empty_args":true}}]}},"storage":{"type":"Storage","engine":{"type":"Function","name":"MergeTree","no_empty_args":true,"kind":"TABLE_ENGINE"},"order_by":{"type":"Identifier","name":"id"}}}'

# Over HTTP: the client may rewrite a `CREATE TABLE` (e.g. add randomized `MergeTree` settings) and send it as SQL,
# which loses the flag.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json" --data-binary "$QUERY" | grep -o -m1 'INCORRECT_QUERY'
$CLICKHOUSE_CLIENT --query "EXISTS TABLE t_lookup_json_flag"
