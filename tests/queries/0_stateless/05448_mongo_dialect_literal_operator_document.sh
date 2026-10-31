#!/usr/bin/env bash
# The argument of `$literal` is a value, so a document in it whose fields are named like operators
# is a document rather than an operator: MongoDB answers `{$literal: {$add: [2, 3]}}` with the
# document `{"$add": [2, 3]}`. An Extended JSON wrapper of a BSON type inside it is still the value
# it wraps, and one of a type that has no counterpart is still refused.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS literal_operator_document;
    CREATE TABLE literal_operator_document (a Int32) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO literal_operator_document VALUES (1);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1"
}

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- an operator-shaped document is a document'
mongo 'db.literal_operator_document.aggregate([{"$project" : {"_id" : 0, "document" : {"$literal" : {"$add" : [2, 3]}}}}]);'

echo '-- also nested'
mongo 'db.literal_operator_document.aggregate([{"$project" : {"_id" : 0, "document" : {"$literal" : {"x" : {"$add" : [2, 3]}, "y" : [{"$size" : "$a"}]}}}}]);'

echo '-- a wrapper of a BSON type is the value it wraps'
mongo 'db.literal_operator_document.aggregate([{"$project" : {"_id" : 0, "document" : {"$literal" : {"id" : {"$oid" : "5f1d7f1c9d3b2a1c4e8b4567"}}}}}]);'

echo '-- a wrapper of a BSON type without a counterpart is refused'
run 'db.literal_operator_document.aggregate([{"$project" : {"_id" : 0, "document" : {"$literal" : {"t" : {"$timestamp" : {"t" : 1, "i" : 1}}}}}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE literal_operator_document;"
