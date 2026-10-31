#!/usr/bin/env bash
# The semantics of the `mongo` dialect fixed after a review:
# - an empty filter `{}` in a logical operator matches every document, as it does at the top level;
# - `$expr` converts its value to a boolean the way Mongo does;
# - `$switch` without a `default` fails for a document no branch matches;
# - `$toString` writes a date as ISO 8601 in UTC;
# - `$regexFind` answers `null` for the capture of a group that did not take part in the match;
# - `$push`, `$firstN` and `$lastN` after a `$sort` keep the order of the elements of an unwound array;
# - `$dateToString` checks the size of its translation before copying the date for each piece of
#   the format.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_review;
    CREATE TABLE mongo_review (id Int32, a Int32, s String, tags Array(Int32), d DateTime64(3, 'Asia/Tokyo')) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_review VALUES (1, 1, '', [20, 10], '2018-03-28 01:58:51.538'), (2, 2, 'false', [], '2020-01-01 09:00:00');
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1"
}

# Prints the error code of a query, which is what the reference records.
mongo_error() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | grep -o -m1 '([A-Z_]*)'
}

echo '-- an empty filter in a logical operator'
mongo 'db.mongo_review.find({"$or" : [{}, {"a" : 1}]}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo 'db.mongo_review.find({"$and" : [{}, {"a" : 1}]}, {"_id" : 0, "id" : 1});'
mongo 'db.mongo_review.find({"$nor" : [{}]}, {"_id" : 0, "id" : 1});'
mongo 'db.mongo_review.find({"$or" : [{}, {}]}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'

echo '-- the truth of $expr'
mongo 'db.mongo_review.find({"$expr" : "$s"}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo 'db.mongo_review.find({"$expr" : "$tags"}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo 'db.mongo_review.find({"$expr" : {"$subtract" : ["$a", 1]}}, {"_id" : 0, "id" : 1});'
mongo 'db.mongo_review.find({"$expr" : null}, {"_id" : 0, "id" : 1});'

echo '-- $switch without a default'
mongo 'db.mongo_review.aggregate([{"$match" : {"a" : 1}}, {"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : {"$eq" : ["$a", 1]}, "then" : "one"}]}}}}]);'
mongo_error 'db.mongo_review.aggregate([{"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : {"$eq" : ["$a", 1]}, "then" : "one"}]}}}}]);'

echo '-- $toString of a date'
mongo 'db.mongo_review.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "column" : {"$toString" : "$d"}, "literal" : {"$toString" : {"$date" : "2018-03-27T16:58:51.538Z"}}, "number" : {"$toString" : "$a"}}}]);'

echo '-- the captures of $regexFind'
mongo 'db.mongo_review.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$regexFind" : {"input" : "b", "regex" : "(a)?b"}}}}]);'
mongo 'db.mongo_review.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$regexFind" : {"input" : "b", "regex" : "(a*)b"}}}}]);'
mongo 'db.mongo_review.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$regexFind" : {"input" : "ab", "regex" : "(x)|(a)(b)"}}}}]);'
mongo 'db.mongo_review.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$regexFind" : {"input" : "a(b", "regex" : "[(]?(b)"}}}}]);'

echo '-- the order of an unwound array'
mongo 'db.mongo_review.aggregate([{"$match" : {"id" : 1}}, {"$sort" : {"id" : 1}}, {"$unwind" : "$tags"}, {"$group" : {"_id" : null, "p" : {"$push" : "$tags"}}}]);'
mongo 'db.mongo_review.aggregate([{"$match" : {"id" : 1}}, {"$sort" : {"id" : -1}}, {"$unwind" : "$tags"}, {"$group" : {"_id" : null, "p" : {"$push" : "$tags"}, "f" : {"$firstN" : {"input" : "$tags", "n" : 1}}, "l" : {"$lastN" : {"input" : "$tags", "n" : 1}}}}]);'

echo '-- the size of the translation of $dateToString'
inner='"$d"'
for _ in $(seq 1 8); do inner="{\"\$toDate\" : ${inner}}"; done
format=$(printf '%%L%.0s' $(seq 1 1000))
mongo_error "db.mongo_review.aggregate([{\"\$project\" : {\"_id\" : 0, \"t\" : {\"\$dateToString\" : {\"date\" : ${inner}, \"format\" : \"${format}\"}}}}]);"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_review"
