#!/usr/bin/env bash
# The operand of `$inc` and `$mul` has to be a number: a JSON number or a numeric Extended JSON
# wrapper. A `null`, a boolean, a string or an array is refused before any mutation is issued,
# rather than turned into `plus(x, NULL)`, which would write `NULL` and acknowledge the update.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS arithmetic_operands;
    CREATE TABLE arithmetic_operands (id Int64, x Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO arithmetic_operands VALUES (1, 5);
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --enable_mongo_dialect 1 --mutations_sync 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the operands that are not numbers'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$inc" : {"x" : null}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$inc" : {"x" : true}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$inc" : {"x" : "1"}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$inc" : {"x" : [1]}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$mul" : {"x" : null}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$mul" : {"x" : false}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$mul" : {"x" : "2"}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$mul" : {"x" : {"$oid" : "5f2a1b3c4d5e6f7a8b9c0d1e"}}});'

echo '-- the row is unchanged'
${CLICKHOUSE_CLIENT} --query "SELECT id, x FROM arithmetic_operands ORDER BY id"

echo '-- a number and a numeric wrapper are applied'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$inc" : {"x" : 2}});'
run 'db.arithmetic_operands.updateMany({"id" : 1}, {"$mul" : {"x" : {"$numberLong" : "3"}}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, x FROM arithmetic_operands ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE arithmetic_operands"
