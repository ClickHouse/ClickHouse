#!/usr/bin/env bash
# Semantics of the Mongo dialect that its first translation got wrong without an error:
# - `$round` rounds a tie to even for integers and decimals as well, not only for floats;
# - a constant key of `$group` is a value, never the position of a column of the select list;
# - `$stdDevSamp` of a single value is `null`;
# - `$first` and `$last` after a `$sort` and a `$unwind` follow the order of the array elements;
# - `$toDecimal` of an embedded document is an error rather than the document;
# - a date moved by milliseconds keeps them even when its column is a `DateTime` or a `Date`, and
#   the terms of `$add` after the date are rounded one by one;
# - a string of a `find` projection is a literal, and a `$` prefixed one a field path;
# - `max_query_size` bounds a Mongo query, and a pattern of many nested optional groups and an
#   operator with very many terms are refused or answered without quadratic work.
#
# Each rejected query runs on its own rather than in a `.sql` file with `-- { clientError ... }`
# hints: a comment is part of the query text in the Mongo dialect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS review_numbers;
    DROP TABLE IF EXISTS review_tags;
    DROP TABLE IF EXISTS review_dates;
    CREATE TABLE review_numbers (id Int64, x Float64, k String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO review_numbers VALUES (1, 5, 'a'), (2, 1, 'b'), (3, 3, 'b');
    CREATE TABLE review_tags (id Int64, tags Array(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO review_tags VALUES (1, [20, 10]);
    CREATE TABLE review_dates (id Int64, d DateTime('UTC'), day Date, d1 DateTime64(1, 'UTC')) ENGINE = MergeTree ORDER BY id;
    INSERT INTO review_dates VALUES (1, '2026-01-01 00:00:00', '2026-01-01', '2026-01-01 00:00:00.0');
"

MONGO_CLIENT="${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1"

run() {
    ${MONGO_CLIENT} --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- $round rounds a tie to even'
${MONGO_CLIENT} --query 'db.review_numbers.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "i" : {"$round" : [25, -1]}, "j" : {"$round" : [35, -1]}, "d" : {"$round" : [{"$numberDecimal" : "2.5"}, 0]}, "f" : {"$round" : [2.5, 0]}}}]);'

echo '-- constant group keys'
${MONGO_CLIENT} --query 'db.review_numbers.aggregate([{"$group" : {"_id" : 0, "n" : {"$sum" : 1}}}]);'
${MONGO_CLIENT} --query 'db.review_numbers.aggregate([{"$group" : {"_id" : 2, "n" : {"$sum" : 1}}}]);'
${MONGO_CLIENT} --query 'db.review_numbers.aggregate([{"$group" : {"_id" : {"a" : 1, "k" : "$k"}, "n" : {"$sum" : 1}}}, {"$sort" : {"n" : 1}}]);'

echo '-- $stdDevSamp of one value is null'
${MONGO_CLIENT} --query 'db.review_numbers.aggregate([{"$group" : {"_id" : "$k", "s" : {"$stdDevSamp" : "$x"}}}, {"$sort" : {"_id" : 1}}]);'

echo '-- $first and $last after $unwind follow the array'
${MONGO_CLIENT} --query 'db.review_tags.aggregate([{"$sort" : {"id" : 1}}, {"$unwind" : "$tags"}, {"$group" : {"_id" : null, "first" : {"$first" : "$tags"}, "last" : {"$last" : "$tags"}}}]);'

echo '-- $toDecimal of a document'
run 'db.review_numbers.aggregate([{"$project" : {"_id" : 0, "d" : {"$toDecimal" : {"$literal" : {"a" : 1}}}}}]);' | grep -o 'only exact for an integer or a boolean'

echo '-- dates moved by milliseconds'
${MONGO_CLIENT} --query 'db.review_dates.aggregate([{"$project" : {"_id" : 0, "a" : {"$add" : ["$d", 1]}, "b" : {"$add" : ["$day", 1000]}, "c" : {"$add" : ["$d1", 1]}, "s" : {"$subtract" : ["$d", 1]}}}]);'
${MONGO_CLIENT} --query 'db.review_dates.aggregate([{"$project" : {"_id" : 0, "after" : {"$add" : ["$d", 1.6, 1.6]}, "before" : {"$add" : [1.6, 1.6, "$d"]}}}]);'

echo '-- find projection strings'
${MONGO_CLIENT} --query 'db.review_numbers.find({"id" : 1}, {"_id" : 0, "label" : "k", "value" : "$k"});'

echo '-- limits'
${MONGO_CLIENT} --max_query_size 100 --query "db.review_numbers.find({\"k\" : \"$(printf 'a%.0s' {1..200})\"});" 2>&1 | grep -o -m1 'QUERY_IS_TOO_LARGE'
${MONGO_CLIENT} --max_query_size 0 --query "db.review_numbers.find({\"k\" : {\"\$ne\" : \"$(printf 'a%.0s' {1..200})\"}}, {\"_id\" : 0, \"id\" : 1}).sort({\"id\" : 1});"
operands=$(printf '1,%.0s' {1..10001})
run "db.review_numbers.aggregate([{\"\$project\" : {\"_id\" : 0, \"m\" : {\"\$multiply\" : [${operands}1]}}}]);"
pattern="$(printf '(%.0s' {1..5000})a$(printf ')?%.0s' {1..5000})"
# The translation finds the optional groups in linear time, and the regular expression library
# refuses a pattern this deep - by its number of groups or the depth of its recursion.
run "db.review_numbers.aggregate([{\"\$match\" : {\"id\" : 1}}, {\"\$project\" : {\"_id\" : 0, \"m\" : {\"\$regexFind\" : {\"input\" : \"b\", \"regex\" : \"${pattern}\"}}}}]);" | grep -c -m1 'Code:'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE review_numbers;
    DROP TABLE review_tags;
    DROP TABLE review_dates;
"
