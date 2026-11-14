#!/usr/bin/env bash
# The Mongo dialect:
# - takes the `$size` of a `Dynamic` field by the type of its value in each row, both in a filter and
#   in an aggregation expression;
# - rejects a dot in a field name of an inserted document, which would alias a nested field.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --enable_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_size_dyn;
    DROP TABLE IF EXISTS mongo_dotted;
    CREATE TABLE mongo_size_dyn (id Int32, a Dynamic, b Array(Int32)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_size_dyn VALUES (1, NULL, []), (2, [1, 2], [1]), (3, 'ab', [1, 2]), (4, 5, []), (5, [[1], [2, 3]], [3]);
    CREATE TABLE mongo_dotted (\`a.b\` Int32) ENGINE = MergeTree ORDER BY tuple();
"

for short_circuit in enable disable; do
    echo "-- \$size of a Dynamic in a filter, short_circuit_function_evaluation = ${short_circuit}"
    mongo --short_circuit_function_evaluation "${short_circuit}" --query 'db.mongo_size_dyn.find({"a" : {"$size" : 2}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
    mongo --short_circuit_function_evaluation "${short_circuit}" --query 'db.mongo_size_dyn.find({"b" : {"$size" : 2}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'

    echo "-- \$size of a Dynamic in an expression, short_circuit_function_evaluation = ${short_circuit}"
    mongo --short_circuit_function_evaluation "${short_circuit}" --query 'db.mongo_size_dyn.aggregate([{"$match" : {"id" : {"$in" : [2, 5]}}}, {"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "n" : {"$size" : "$a"}}}]);'
    mongo --short_circuit_function_evaluation "${short_circuit}" --query 'db.mongo_size_dyn.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "n" : {"$size" : "$b"}}}]);'
    err --short_circuit_function_evaluation "${short_circuit}" --query 'db.mongo_size_dyn.aggregate([{"$project" : {"_id" : 0, "n" : {"$size" : "$a"}}}]);'
done

echo '-- a dot in a field name of an inserted document'
err --query 'db.mongo_dotted.insertOne({"a.b" : 1});'
err --query 'db.mongo_dotted.insertOne({"a" : {"b.c" : 1}});'
mongo --query 'db.mongo_dotted.insertOne({"a" : {"b" : 2}});' > /dev/null
mongo --query 'db.mongo_dotted.find({}, {"_id" : 0});'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_size_dyn;
    DROP TABLE mongo_dotted;
"
