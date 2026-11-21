#!/usr/bin/env bash
# Semantics of the Mongo dialect:
# - `$trim`, `$ltrim` and `$rtrim` trim the whitespace code points of Mongo, not only the ASCII space,
#   and do not cut a multi-byte character apart;
# - `$setUnion`, `$setDifference` and `$setEquals` keep `null` as an element of a set;
# - `$divide` of two integers is a double whatever `sql_compatible_division_for_integers` says;
# - the filter of a `find` reads the field of the input, not the value its projection computes;
# - `$toDate` of a decimal truncates it rather than rounding it through a double;
# - the filter of a delete counts towards `max_ast_elements`;
# - the terms of `$add` that are copied for each possible date count towards the size of the translation.
#
# Each rejected query runs on its own rather than in a `.sql` file with `-- { clientError ... }`
# hints: a comment is part of the query text in the Mongo dialect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_review_t;
    CREATE TABLE mongo_review_t (id Int64, a Int64, s String, arr Array(Nullable(Int64))) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_review_t VALUES (1, 1, concat(char(9), ' x', char(10), unhex('C2A0')), [1, NULL]), (2, 2, '¢y¢', []);
"

MONGO_CLIENT="${CLICKHOUSE_CLIENT} --dialect mongo --enable_mongo_dialect 1 --max_threads 1"

run() {
    ${MONGO_CLIENT} "$@" 2>&1 >/dev/null \
        | head -1 | grep -o -m1 '([A-Z_]*)'
}

echo '-- $trim'
${MONGO_CLIENT} --query 'db.mongo_review_t.aggregate([{$sort: {id: 1}}, {$project: {_id: 0, t: {$trim: {input: "$s"}}, l: {$ltrim: {input: "$s"}}, r: {$rtrim: {input: "$s"}}}}])' --format JSONEachRow

echo '-- sets with null'
${MONGO_CLIENT} --query 'db.mongo_review_t.aggregate([{$match: {id: 1}}, {$project: {_id: 0, eq: {$setEquals: [[1, null], [1]]}, eq_field: {$setEquals: ["$arr", [null, 1, 1]]}, u: {$setUnion: ["$arr", [null, 2]]}, d: {$setDifference: [[null, 1], [1]]}}}])' --format JSONEachRow

echo '-- $divide'
for setting in 0 1; do
    ${MONGO_CLIENT} --sql_compatible_division_for_integers "$setting" --query 'db.mongo_review_t.aggregate([{$match: {id: 1}}, {$project: {_id: 0, d: {$divide: [5, 2]}, f: {$divide: ["$a", 2]}}}])' --format JSONEachRow
    ${MONGO_CLIENT} --sql_compatible_division_for_integers "$setting" --query 'db.mongo_review_t.find({id: 1}, {_id: 0, d: {$divide: [5, 2]}})' --format JSONEachRow
done

echo '-- the filter of a find reads the input'
${MONGO_CLIENT} --query 'db.mongo_review_t.find({a: 99}, {_id: 0, a: {$literal: 99}})' --format JSONEachRow
${MONGO_CLIENT} --query 'db.mongo_review_t.find({a: 1}, {_id: 0, a: {$literal: 99}})' --format JSONEachRow
${MONGO_CLIENT} --query 'db.mongo_review_t.find({}, {_id: 0, a: {$multiply: ["$a", -1]}}).sort({a: 1})' --format JSONEachRow

echo '-- $toDate of a decimal'
${MONGO_CLIENT} --query 'db.mongo_review_t.aggregate([{$match: {id: 1}}, {$project: {_id: 0, t: {$toLong: {$toDate: {$numberDecimal: "1546300800000.999999999"}}}}}])' --format JSONEachRow

echo '-- the filter of a delete is a part of the query'
run --max_ast_elements 20 --query 'db.mongo_review_t.deleteMany({a: {$in: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}})'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_review_t"

echo '-- the copies of the terms of $add'
# Each level of `$toDate` refers to its argument several times, so three of them are a large
# expression that is still under the bound, and `$add` would copy each of its terms for each term.
expression='"$a"'
for _ in $(seq 1 3); do expression="{\$toDate: $expression}"; done
terms="$expression"
for _ in $(seq 2 32); do terms="$terms, $expression"; done
${MONGO_CLIENT} --query "db.mongo_review_t.aggregate([{\$match: {id: 1}}, {\$project: {_id: 0, x: {\$add: [$expression, 1]}}}])" --format Null && echo 'a date and a number: ok'
run --query "db.mongo_review_t.aggregate([{\$project: {_id: 0, x: {\$add: [$terms]}}}])"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_review_t"
