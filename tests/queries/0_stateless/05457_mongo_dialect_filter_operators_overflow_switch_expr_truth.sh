#!/usr/bin/env bash
# The Mongo dialect:
# - rejects an expression operator such as `$add` at the top level of a filter, which would
#   otherwise become a constant true condition and match - or delete - every document;
# - keeps booleans and numbers apart in `$all`, `$pull` and `$pullAll`;
# - takes the Mongo truth value of the expression of `$expr`;
# - fails a `$switch` without a `default` when no branch matches, rather than returning `null`;
# - fails an integer `$sum`, `$add`, `$subtract` or `$multiply` that does not fit a 64-bit integer,
#   rather than returning the wrapped around value;
# - rejects an arithmetic operator with so many operands that their nesting is too deep.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_t;
    CREATE TABLE mongo_t (id Int32, v Int64, s String, n Nullable(Int32), flags Array(Bool), nums Array(Int32)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, 9223372036854775807, '', NULL, [true, false], [1, 0, 2]), (2, 1, 'x', 0, [false], [3]);
"

ids() {
    mongo --query "db.mongo_t.find($1, {\"_id\" : 0, \"id\" : 1}).sort({\"id\" : 1});" | tr -d '{}"id:' | tr '\n' ' '
    echo
}

echo '-- top level operators of a filter'
err --query 'db.mongo_t.find({"$add" : [1, 2]});'
err --query 'db.mongo_t.find({"id" : 1, "$mul" : [1, 2]});'
err --mutations_sync 2 --query 'db.mongo_t.deleteMany({"$add" : [1, 2]});'
err --mutations_sync 2 --query 'db.mongo_t.updateMany({"$sub" : [1, 2]}, {"$set" : {"s" : "y"}});'
err --query 'db.mongo_t.aggregate([{"$match" : {"$div" : [1, 2]}}]);'
${CLICKHOUSE_CLIENT} --query "SELECT id, s FROM mongo_t ORDER BY id"
ids '{"$or" : [{"id" : 1}], "$comment" : "c"}'
mongo --query 'db.mongo_t.find({"id" : 2}, {"_id" : 0, "x" : {"$add" : [{"$add" : ["id", 1]}, 1]}});'
err --query 'db.mongo_t.find({}, {"x" : {"$add" : [1, {"$comment" : "c"}]}});'

echo '-- $all, $pull and $pullAll'
ids '{"flags" : {"$all" : [1]}}'
ids '{"flags" : {"$all" : [true]}}'
ids '{"flags" : {"$all" : [false, true]}}'
ids '{"nums" : {"$all" : [true]}}'
ids '{"nums" : {"$all" : [0, 1]}}'
err --mutations_sync 2 --query 'db.mongo_t.deleteMany({"flags" : {"$all" : [1]}});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_t"
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 1}, {"$pull" : {"flags" : 1, "nums" : true}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, flags, nums FROM mongo_t ORDER BY id"
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 1}, {"$pullAll" : {"flags" : [1, 0], "nums" : [true, 2]}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, flags, nums FROM mongo_t ORDER BY id"
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 1}, {"$pullAll" : {"flags" : [true, 0], "nums" : [false, 1]}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, flags, nums FROM mongo_t ORDER BY id"

echo '-- the truth value of $expr'
ids '{"$expr" : ""}'
ids '{"$expr" : "$s"}'
ids '{"$expr" : "$n"}'
ids '{"$expr" : 0}'
ids '{"$expr" : 2}'
ids '{"$expr" : null}'
ids '{"$expr" : {"$eq" : ["$id", 2]}}'
ids '{"$expr" : {"$add" : ["$id", -1]}}'
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"$expr" : "$s"}, {"$set" : {"s" : "expr"}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, s FROM mongo_t ORDER BY id"
mongo --mutations_sync 2 --query 'db.mongo_t.deleteMany({"$expr" : "$n"});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_t"

echo '-- $switch without a default'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : false, "then" : 1}]}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : {"$gt" : ["$id", 0]}, "then" : "matched"}]}}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : {"$eq" : ["$id", 1]}, "then" : "one"}]}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : false, "then" : 1}], "default" : null}}}}]);'

echo '-- integer overflow'
err --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "total" : {"$sum" : "$v"}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "total" : {"$sum" : "$id"}, "count" : {"$sum" : 1}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$match" : {"id" : 1}}, {"$group" : {"_id" : null, "total" : {"$sum" : "$v"}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "a" : {"$add" : ["$v", 1]}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "a" : {"$subtract" : [{"$multiply" : ["$v", -1]}, 2]}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "a" : {"$multiply" : ["$v", 2]}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "a" : {"$add" : ["$v", 0]}, "b" : {"$subtract" : ["$v", 1]}, "c" : {"$multiply" : ["$id", 3, 0.5]}, "d" : {"$add" : ["$v", -1, 1]}}}]);'

echo '-- too many operands'
operands=$(python3 -c "print(', '.join(['1'] * 1001))")
err --query "db.mongo_t.find({}, {\"x\" : {\"\$add\" : [${operands}]}});"
err --query "db.mongo_t.aggregate([{\"\$project\" : {\"x\" : {\"\$add\" : [${operands}]}}}]);"
operands=$(python3 -c "print(', '.join(['1'] * 100))")
mongo --query "db.mongo_t.aggregate([{\"\$limit\" : 1}, {\"\$project\" : {\"_id\" : 0, \"x\" : {\"\$add\" : [${operands}]}}}]);"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_t"
