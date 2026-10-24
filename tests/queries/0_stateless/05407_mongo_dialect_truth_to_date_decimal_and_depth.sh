#!/usr/bin/env bash
# The semantics of the `mongo` dialect where the ClickHouse functions it is lowered into differ:
#
# - `$and`, `$or`, `$not` and the conditions of `$cond`, `$switch` and `$filter` read a value the
#   way Mongo does: `null` is false rather than `NULL`, and a string or an array is true;
# - `$toDate` reads a number as milliseconds also when the column is `Nullable`;
# - `$toLong` of a decimal truncates it exactly, above 2^53 as well;
# - a request nested deeper than `max_parser_depth` is refused before it is parsed.
#
# The errors run through the shell rather than through a `-- { serverError ... }` hint: a comment
# is part of the query text in the Mongo dialect, so an annotation would change the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS truth;
    CREATE TABLE truth (id Int64, n Nullable(Int64), s String, arr Array(Int64), ms Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO truth VALUES (1, NULL, '', [], 1000), (2, 0, 'x', [0], NULL), (3, 5, 'false', [1, 2], 1546300800000);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 \
        | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/^Code: [0-9]*\. //' -e 's/DB::Exception: //' \
        | grep -v '^$'
}

# The name of the error is what the reference records: the message quotes the query.
mongo_error() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 "${@:2}" --query "$1" 2>&1 >/dev/null \
        | grep -oE '\(([A-Z_]+)\)' | tail -1
}

echo '-- the truth of a value in a boolean operator'
mongo 'db.truth.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "not_n" : {"$not" : ["$n"]}, "and_n" : {"$and" : ["$n", true]}, "or_n" : {"$or" : ["$n", false]}, "not_s" : {"$not" : ["$s"]}, "and_arr" : {"$and" : ["$arr", "$s"]}}}]);'
mongo 'db.truth.aggregate([{"$limit" : 1}, {"$project" : {"_id" : 0, "not_null" : {"$not" : [null]}, "and_null" : {"$and" : [null, true]}, "or_null" : {"$or" : [null, false]}, "and_none" : {"$and" : []}, "or_none" : {"$or" : []}, "and_one" : {"$and" : ["false"]}}}]);'

echo '-- the condition of cond, switch and filter'
mongo 'db.truth.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "cond" : {"$cond" : ["$n", "yes", "no"]}, "switch" : {"$switch" : {"branches" : [{"case" : "$n", "then" : "n"}, {"case" : "$s", "then" : "s"}], "default" : "none"}}, "filtered" : {"$filter" : {"input" : [null, 0, 1, "", "a"], "cond" : "$$this"}}}}]);'

echo '-- toDate of a nullable number is milliseconds'
mongo 'db.truth.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "d" : {"$toDate" : "$ms"}}}]);'

echo '-- toLong of a decimal is exact'
mongo 'db.truth.aggregate([{"$limit" : 1}, {"$project" : {"_id" : 0, "big" : {"$toLong" : {"$numberDecimal" : "9007199254740993"}}, "negative" : {"$toLong" : {"$numberDecimal" : "-9007199254740993.9"}}, "fraction" : {"$toLong" : {"$numberDecimal" : "2.7"}}, "small_negative" : {"$toLong" : {"$numberDecimal" : "-0.5"}}}}]);'
mongo_error 'db.truth.aggregate([{"$project" : {"_id" : 0, "l" : {"$toLong" : {"$numberDecimal" : "9223372036854775808"}}}}]);'

echo '-- a request nested too deeply'
deep=$(printf '[%.0s' $(seq 1 200))1$(printf ']%.0s' $(seq 1 200))
mongo_error "db.truth.insertOne({\"id\" : 4, \"arr\" : ${deep}});" --max_parser_depth 100
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM truth"

${CLICKHOUSE_CLIENT} --query "DROP TABLE truth"
