#!/usr/bin/env bash
# The semantics of the `mongo` dialect where the ClickHouse functions it is lowered into differ:
#
# - a regular expression's dot matches a newline only with the option `s`, and `x` drops whitespace
#   and comments, also in the filter of a mutation;
# - the comparisons of the aggregation language compare `null` as a value;
# - `$arrayElemAt`, `$first` and `$last` take no element past the ends of an array;
# - `$size` sees the array a path of a `JSON` column holds;
# - `$min` and `$max` of an update order `null` below every value;
# - a nested document of `$project` projects the fields below it;
# - a sort on an array column is refused, and so is an ordered accumulator after an `$unwind`
#   replaced a sort key;
# - the chained `.sort` and a long flat pipeline are bounded by `max_parser_depth`.
#
# The errors run through the shell rather than through a `-- { serverError ... }` hint: a comment
# is part of the query text in the Mongo dialect, so an annotation would change the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_t;
    DROP TABLE IF EXISTS mongo_j;
    DROP TABLE IF EXISTS mongo_p;
    CREATE TABLE mongo_t (id Int64, s String, a Array(Int64), n Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, 'a\nb', [10], NULL), (2, 'ab', [], 1), (3, 'a b', [2, 10], 5);
    CREATE TABLE mongo_j (id Int64, json JSON) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_j VALUES (1, '{\"tags\" : [1, 2]}'), (2, '{\"tags\" : 5}'), (3, '{\"tags\" : \"ab\"}'), (4, '{}');
    CREATE TABLE mongo_p (id String, \`profile.name\` String, \`profile.age\` Int64, tags Array(Int64), x Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_p VALUES ('A', 'alice', 30, [1, 100], 5), ('B', 'bob', 40, [2, 3], NULL);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --mutations_sync 2 "${@:2}" --query "$1" 2>&1 \
        | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/^Code: [0-9]*\. //' -e 's/DB::Exception: //' \
        | grep -v '^$'
}

# The name of the error is what the reference records: the message quotes the query.
mongo_error() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 "${@:2}" --query "$1" 2>&1 >/dev/null \
        | grep -oE '\(([A-Z_]+)\)' | tail -1
}

echo '-- the dot of a regular expression and a newline'
mongo 'db.mongo_t.find({"s" : {"$regex" : "a.b"}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo 'db.mongo_t.find({"s" : {"$regex" : "a.b", "$options" : "s"}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo 'db.mongo_t.find({"s" : {"$regex" : "(?s)a.b"}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "m" : {"$regexMatch" : {"input" : "$s", "regex" : "^a.b$"}}}}]);'

echo '-- the extended mode'
mongo 'db.mongo_t.find({"s" : {"$regex" : "a b  # a comment", "$options" : "x"}}, {"_id" : 0, "id" : 1});'
mongo 'db.mongo_t.find({"s" : {"$regex" : "a[ ]b|a\\ b", "$options" : "x"}}, {"_id" : 0, "id" : 1});'

echo '-- comparisons of null'
mongo 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "eq" : {"$eq" : ["$n", null]}, "ne" : {"$ne" : ["$n", 1]}, "lt" : {"$lt" : ["$n", 2]}, "lte" : {"$lte" : [null, "$n"]}, "gt" : {"$gt" : ["$n", null]}, "gte" : {"$gte" : ["$n", null]}, "cmp" : {"$cmp" : ["$n", 1]}, "null_null" : {"$eq" : [null, null]}}}]);'
mongo 'db.mongo_t.find({"$expr" : {"$not" : [{"$eq" : ["$n", null]}]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'

echo '-- an element past the ends of an array'
mongo 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "at_2" : {"$arrayElemAt" : ["$a", 2]}, "at_0" : {"$arrayElemAt" : ["$a", 0]}, "at_minus_3" : {"$arrayElemAt" : ["$a", -3]}, "at_n" : {"$arrayElemAt" : ["$a", "$n"]}, "first" : {"$first" : "$a"}, "last" : {"$last" : "$a"}}}]);'

echo '-- size of a path of a JSON column'
mongo 'db.mongo_j.find({"json.tags" : {"$size" : 2}}, {"_id" : 0, "id" : 1});'
mongo 'db.mongo_j.find({"json.tags" : {"$not" : {"$size" : 2}}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'

echo '-- min and max of null'
for operator in min max; do
    for value in null 1 10; do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS mongo_u; CREATE TABLE mongo_u (id Int64, x Nullable(Int64)) ENGINE = MergeTree ORDER BY id; INSERT INTO mongo_u VALUES (1, 5), (2, NULL)"
        mongo "db.mongo_u.updateMany({}, {\"\$${operator}\" : {\"x\" : ${value}}});"
        echo "${operator} ${value}: $(${CLICKHOUSE_CLIENT} --query "SELECT x FROM mongo_u ORDER BY id" | tr '\n' ' ')"
    done
done
${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_u"

echo '-- a nested projection'
mongo 'db.mongo_p.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "profile" : {"name" : 1}}}]);'
mongo 'db.mongo_p.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"profile" : {"age" : 0}, "tags" : 0}}]);'

echo '-- a sort on an array'
mongo_error 'db.mongo_p.find().sort({"tags" : 1});'
mongo_error 'db.mongo_p.aggregate([{"$sort" : {"tags" : -1}}]);'
mongo 'db.mongo_p.find({}, {"_id" : 0, "id" : 1}).sort({"id" : -1}).limit(1);'

echo '-- an unwind that replaces a sort key'
mongo_error 'db.mongo_p.aggregate([{"$sort" : {"x" : 1}}, {"$unwind" : {"path" : "$tags", "includeArrayIndex" : "x"}}, {"$group" : {"_id" : null, "last" : {"$last" : "$id"}}}]);'
mongo 'db.mongo_p.aggregate([{"$sort" : {"id" : 1}}, {"$unwind" : "$tags"}, {"$group" : {"_id" : null, "ids" : {"$push" : "$id"}, "last" : {"$last" : "$id"}}}]);'

echo '-- the depth of a request'
deep=$(printf '[%.0s' $(seq 1 40))1$(printf ']%.0s' $(seq 1 40))
mongo_error "db.mongo_p.find().sort({\"id\" : ${deep}});" --max_parser_depth 30
stages=$(printf '{"$skip" : 0},%.0s' $(seq 1 40))
mongo_error "db.mongo_p.aggregate([${stages%,}]);" --max_parser_depth 30
mongo "db.mongo_p.aggregate([${stages%,}, {\"\$count\" : \"c\"}]);"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_t; DROP TABLE mongo_j; DROP TABLE mongo_p"
