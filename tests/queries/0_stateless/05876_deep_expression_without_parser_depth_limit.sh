#!/usr/bin/env bash
# A chain of `+` parses into a tree as deep as the chain. With `max_parser_depth = 0`, or in `INSERT ... VALUES`, nothing
# bounds that depth before `max_ast_depth` is checked, so the walks over the tree before that check must not overflow the stack.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function check()
{
    if ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$2" --data-binary @- | grep -q -E 'TOO_DEEP_RECURSION|TOO_DEEP_AST'
    then echo "$1 OK"; else echo "$1 FAIL"; fi
}

UNLIMITED="max_parser_depth=0&max_parser_backtracks=0"
for n in 15000 16000; do
    python3 -c "print('SELECT ' + '1+' * $n + '1')" | check "direct $n" "$UNLIMITED"
done
python3 -c "print(\"SELECT 1 SETTINGS filter = '\" + '1+' * 100000 + \"1'\")" | check "filter" "$UNLIMITED"
for n in 20000 200000; do
    echo "SELECT * FROM eval(concat('SELECT ', repeat('1+', $n), '1'))" \
        | check "eval $n" "$UNLIMITED&enable_eval_table_function=1&max_query_size=1000000"
done

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "CREATE TABLE t (x UInt64) ENGINE = Memory"
python3 -c "print('INSERT INTO t VALUES (' + '1+' * 20000 + '1)')" | check "values" ""
python3 -c "print('INSERT INTO t VALUES ((SELECT ' + '1+' * 200000 + '1))')" | check "values subquery" ""
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM t"
