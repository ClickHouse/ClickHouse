#!/usr/bin/env bash
# - `$not`, `$and`, `$or`, `$cond`, `$switch` and `$filter` take the Mongo truth value of their
#   conditions: `null` is false and a string is true; the query `$not` and `$nor` match a document
#   whose field is `null`;
# - `$toString` of a date is its ISO 8601 text in UTC, and `$stdDevSamp` of one value is `null`;
# - `$min` and `$max` updates order `null` below every value, whatever
#   `least_greatest_legacy_null_behavior` is;
# - a decimal or a double assigned by an update has to be representable exactly by the column, and
#   is rejected before a mutation is created;
# - an insert of an empty embedded document is rejected;
# - nested `$size`, `$slice`, `$toDecimal` and `$dateToString` do not grow the query exponentially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_t;
    CREATE TABLE mongo_t (id Int32, v Nullable(Int32), s String, d DateTime64(3, 'Asia/Tokyo'), x Int32, dec Decimal(38, 1), profile String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, NULL, 'green', 1.5, 1, 1, ''), (2, 7, '', 0, 2, 2, '');
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

echo '-- truth values'
mongo --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "not" : {"$not" : ["$v"]}, "not_null" : {"$not" : [null]}, "and" : {"$and" : [1, "$s"]}, "or" : {"$or" : [null, 0]}, "and_none" : {"$and" : []}, "or_one" : {"$or" : ["$v"]}, "cond" : {"$cond" : ["$s", 1, 2]}, "switch" : {"$switch" : {"branches" : [{"case" : "$v", "then" : "v"}], "default" : "none"}}, "filter" : {"$filter" : {"input" : [1, 0, null, 2], "cond" : "$$this"}}}}]);'
mongo --query 'db.mongo_t.find({"$expr" : {"$not" : ["$v"]}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"v" : {"$not" : {"$gt" : 5}}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"$nor" : [{"v" : {"$gt" : 5}}]}, {"_id" : 0, "id" : 1});'
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"$expr" : {"$not" : ["$v"]}}, {"$set" : {"profile" : "no v"}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, profile FROM mongo_t ORDER BY id"

echo '-- $toString of a date and $stdDevSamp'
mongo --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "d" : {"$toString" : "$d"}, "date" : {"$toString" : {"$date" : 1500}}, "v" : {"$toString" : "$v"}, "id" : {"$toString" : "$id"}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : "$id", "s" : {"$stdDevSamp" : "$x"}}}, {"$project" : {"_id" : 1, "s" : 1, "f" : {"$ifNull" : ["$s", 99]}}}, {"$sort" : {"_id" : 1}}]);'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "s" : {"$stdDevSamp" : "$v"}, "p" : {"$stdDevSamp" : "$x"}}}]);'

echo '-- $min and $max'
for legacy in 0 1; do
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE mongo_t; INSERT INTO mongo_t (id, v) VALUES (1, NULL), (2, 5), (3, 5), (4, NULL)"
    mongo --mutations_sync 2 --least_greatest_legacy_null_behavior $legacy --query 'db.mongo_t.updateMany({"id" : {"$in" : [1, 2]}}, {"$min" : {"v" : 3}});'
    mongo --mutations_sync 2 --least_greatest_legacy_null_behavior $legacy --query 'db.mongo_t.updateMany({"id" : 3}, {"$min" : {"v" : null}});'
    mongo --mutations_sync 2 --least_greatest_legacy_null_behavior $legacy --query 'db.mongo_t.updateMany({"id" : 4}, {"$max" : {"v" : 4}});'
    mongo --mutations_sync 2 --least_greatest_legacy_null_behavior $legacy --query 'db.mongo_t.updateMany({"id" : 2}, {"$max" : {"v" : null}});'
    ${CLICKHOUSE_CLIENT} --query "SELECT groupArray((id, v)) FROM (SELECT id, v FROM mongo_t ORDER BY id)"
done

echo '-- exact constants'
mutations() {
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'mongo_t'"
}
mutations_before=$(mutations)
err --query 'db.mongo_t.updateMany({}, {"$set" : {"x" : {"$numberDecimal" : "1.5"}}});'
err --query 'db.mongo_t.updateMany({}, {"$set" : {"dec" : {"$numberDecimal" : "1.25"}}});'
err --query 'db.mongo_t.updateMany({}, {"$set" : {"dec" : 1.25}});'
err --query 'db.mongo_t.updateMany({}, {"$inc" : {"dec" : {"$numberDecimal" : "0.05"}}});'
echo "mutations created: $(( $(mutations) - mutations_before ))"
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 1}, {"$set" : {"x" : {"$numberDecimal" : "3"}, "dec" : {"$numberDecimal" : "1.20"}, "s" : 1.5}});'
${CLICKHOUSE_CLIENT} --query "SELECT x, dec, s FROM mongo_t WHERE id = 1"

echo '-- empty embedded document'
err --query 'db.mongo_t.insertOne({"id" : 5, "profile" : {}});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_t WHERE id = 5"

echo '-- bounded growth'
# Five levels stay within the stack budget the analyzer has under TSan, and while the linear
# translation needs at most about 350 AST elements, one that doubled with every level would exceed 600.
nest() {
    python3 -c "
import json, sys
e = json.loads(sys.argv[2])
for _ in range(5):
    e = json.loads(sys.argv[1].replace('E', json.dumps(e)))
print(json.dumps(e))" "$1" "$2"
}
for template in '{"$size" : {"$range" : [0, E]}}' '{"$size" : {"$slice" : [[1, 2, 3], E]}}' '{"$toInt" : {"$toDecimal" : E}}' '{"$strLenBytes" : {"$dateToString" : {"date" : {"$toDate" : E}, "format" : "%L%L"}}}'; do
    mongo --max_ast_elements 600 --query "db.mongo_t.aggregate([{\"\$match\" : {\"id\" : 1}}, {\"\$project\" : {\"_id\" : 0, \"r\" : $(nest "$template" 1)}}]);"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_t"
