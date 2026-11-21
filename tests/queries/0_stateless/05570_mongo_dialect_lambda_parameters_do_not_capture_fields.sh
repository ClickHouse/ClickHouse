#!/usr/bin/env bash
# The lambda parameters that the translation of the `mongo` dialect binds - a variable `$$<name>` of
# `$map` and `$filter`, the element of `$setDifference`, the element appended by `$addToSet` - must
# not capture a field read inside the lambda, whatever the name of that field is, including names
# that look like the internal names the translation used to give these parameters.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS lambda_capture;
    CREATE TABLE lambda_capture (_id String, a Array(Int64), x Int64, __mongo_variable_x Int64, __mongo_element Array(Int64))
        ENGINE = MergeTree ORDER BY _id;
    INSERT INTO lambda_capture VALUES ('d', [1, 2], 100, 10, [2]);
    DROP TABLE IF EXISTS lambda_capture_update;
    CREATE TABLE lambda_capture_update (id Int64, __mongo_element Array(String)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO lambda_capture_update VALUES (1, ['a']);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --enable_mongo_dialect 1 --mutations_sync 2 --query "$1" 2>&1 \
        | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/^Code: [0-9]*\. //' -e 's/DB::Exception: //' \
        | grep -v '^$'
}

echo '-- $$x is the element and $x is the field'
mongo 'db.lambda_capture.aggregate([{"$project" : {"_id" : 0, "m" : {"$map" : {"input" : "$a", "as" : "x", "in" : {"$add" : ["$$x", "$x"]}}}}}]);'
mongo 'db.lambda_capture.aggregate([{"$project" : {"_id" : 0, "f" : {"$filter" : {"input" : "$a", "as" : "x", "cond" : {"$lt" : ["$$x", {"$subtract" : ["$x", 98]}]}}}}}]);'
echo '-- a field named like the old lambda parameter of a variable'
mongo 'db.lambda_capture.aggregate([{"$project" : {"_id" : 0, "m" : {"$map" : {"input" : "$a", "as" : "x", "in" : {"$multiply" : ["$$x", "$__mongo_variable_x"]}}}}}]);'
mongo 'db.lambda_capture.aggregate([{"$project" : {"_id" : 0, "f" : {"$filter" : {"input" : "$a", "as" : "x", "cond" : {"$gt" : ["$__mongo_variable_x", {"$multiply" : ["$$x", 6]}]}}}}}]);'
echo '-- an inner variable shadows an outer one of the same name'
mongo 'db.lambda_capture.aggregate([{"$project" : {"_id" : 0, "m" : {"$map" : {"input" : [[1], [2, 3]], "as" : "x", "in" : {"$map" : {"input" : "$$x", "as" : "x", "in" : {"$add" : ["$$x", "$x"]}}}}}}}]);'
echo '-- a field named like the old lambda parameter of $setDifference'
mongo 'db.lambda_capture.aggregate([{"$project" : {"_id" : 0, "s" : {"$setDifference" : ["$a", "$__mongo_element"]}}}]);'
echo '-- a field named like the old lambda parameter of $addToSet'
mongo 'db.lambda_capture_update.updateMany({"id" : 1}, {"$addToSet" : {"__mongo_element" : {"$each" : ["a", "b"]}}});'
${CLICKHOUSE_CLIENT} --query "SELECT __mongo_element FROM lambda_capture_update"

${CLICKHOUSE_CLIENT} --query "DROP TABLE lambda_capture; DROP TABLE lambda_capture_update"
