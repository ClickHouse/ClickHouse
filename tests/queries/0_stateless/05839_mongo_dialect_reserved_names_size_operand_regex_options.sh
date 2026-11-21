#!/usr/bin/env bash
# The Mongo dialect:
# - reserves the prefix `__mongo_` of the identifiers its translation generates, so that a field of
#   a document cannot be captured by the parameter of a generated lambda;
# - takes only a number as the operand of `$size`;
# - applies the `$options` next to a `$regex` that holds a BSON regular expression.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --enable_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --mutations_sync 2 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_reserved;
    DROP TABLE IF EXISTS mongo_size_operand;
    DROP TABLE IF EXISTS mongo_regex_options;
    CREATE TABLE mongo_reserved (id Int32, x Int32, a Array(Int32), __mongo_variable_x Int32, __mongo_value_1 Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_reserved VALUES (1, 10, [1, 2], 10, 4);
    CREATE TABLE mongo_size_operand (id Int32, a Array(Int32)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_size_operand VALUES (1, [7]), (2, [7, 8]);
    CREATE TABLE mongo_regex_options (id Int32, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_regex_options VALUES (1, 'Alpha'), (2, 'beta');
"

echo '-- a variable and a field of the same name stay apart'
mongo --query 'db.mongo_reserved.aggregate([{"$project" : {"_id" : 0, "r" : {"$map" : {"input" : "$a", "as" : "x", "in" : {"$add" : ["$$x", "$x"]}}}}}]);'

echo '-- a field with the prefix of the generated names cannot be referred to'
err --query 'db.mongo_reserved.aggregate([{"$project" : {"_id" : 0, "r" : {"$map" : {"input" : "$a", "as" : "x", "in" : {"$add" : ["$$x", "$__mongo_variable_x"]}}}}}]);'
err --query 'db.mongo_reserved.aggregate([{"$project" : {"_id" : 0, "r" : {"$divide" : [{"$add" : [1, 1]}, "$__mongo_value_1"]}}}]);'
err --query 'db.mongo_reserved.find({"__mongo_value_1" : 4});'
err --query 'db.mongo_reserved.find({}, {"__mongo_value_1" : 1});'
err --query 'db.mongo_reserved.find({}).sort({"__mongo_value_1" : 1});'
err --query 'db.mongo_reserved.aggregate([{"$unwind" : "$__mongo_element"}]);'
err --query 'db.mongo_reserved.aggregate([{"$unwind" : {"path" : "$a", "includeArrayIndex" : "__mongo_index"}}]);'
err --query 'db.mongo_reserved.updateMany({"id" : 1}, {"$set" : {"__mongo_value_1" : 5}});'
err --query 'db.mongo_reserved.deleteMany({"__mongo_value_1" : 4});'
err --query 'db.mongo_reserved.insertOne({"id" : 2, "__mongo_value_1" : 5});'
echo '-- a string that is not a field path is still a value'
mongo --query 'db.mongo_reserved.aggregate([{"$project" : {"_id" : 0, "r" : {"$concat" : ["__mongo_", "value"]}}}]);'
${CLICKHOUSE_CLIENT} --query "SELECT id, x, a, __mongo_variable_x, __mongo_value_1 FROM mongo_reserved ORDER BY id"

echo '-- $size takes a number'
mongo --query 'db.mongo_size_operand.find({"a" : {"$size" : 1}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_size_operand.find({"a" : {"$size" : {"$numberLong" : "2"}}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_size_operand.find({"a" : {"$size" : {"$numberInt" : "1"}}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_size_operand.find({"a" : {"$size" : 2.0}}, {"_id" : 0, "id" : 1});'
err --query 'db.mongo_size_operand.find({"a" : {"$size" : true}});'
err --query 'db.mongo_size_operand.find({"a" : {"$size" : "1"}});'
err --query 'db.mongo_size_operand.find({"a" : {"$size" : null}});'
err --query 'db.mongo_size_operand.aggregate([{"$match" : {"a" : {"$size" : true}}}]);'
err --query 'db.mongo_size_operand.deleteMany({"a" : {"$size" : true}});'
err --query 'db.mongo_size_operand.deleteMany({"a" : {"$size" : "1"}});'
err --query 'db.mongo_size_operand.updateMany({"a" : {"$size" : true}}, {"$set" : {"id" : 3}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, a FROM mongo_size_operand ORDER BY id"

echo '-- $options next to a BSON regular expression'
mongo --query 'db.mongo_regex_options.find({"name" : {"$regex" : {"$regularExpression" : {"pattern" : "^a", "options" : ""}}, "$options" : "i"}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_regex_options.find({"name" : {"$regex" : {"$regularExpression" : {"pattern" : "^a", "options" : ""}}}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_regex_options.find({"name" : {"$regex" : {"$regularExpression" : {"pattern" : "^a", "options" : "i"}}}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_regex_options.find({"name" : {"$not" : {"$regex" : {"$regularExpression" : {"pattern" : "^a", "options" : ""}}, "$options" : "i"}}}, {"_id" : 0, "id" : 1});'
echo '-- the legacy form of a BSON regular expression, as the wire protocol passes it on'
mongo --query 'db.mongo_regex_options.find({"name" : {"$regex" : {"$regex" : "^a", "$options" : ""}, "$options" : "i"}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_regex_options.find({"name" : {"$regex" : {"$regex" : "^a", "$options" : "i"}}}, {"_id" : 0, "id" : 1});'
err --query 'db.mongo_regex_options.find({"name" : {"$regex" : {"$regex" : "^a", "$options" : "i"}, "$options" : "m"}});'
echo '-- the negated match deletes only the document that does not match'
mongo --query 'db.mongo_regex_options.deleteMany({"name" : {"$not" : {"$regex" : {"$regularExpression" : {"pattern" : "^a", "options" : ""}}, "$options" : "i"}}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, name FROM mongo_regex_options ORDER BY id"
echo '-- options in both places, and options that are not a string'
err --query 'db.mongo_regex_options.find({"name" : {"$regex" : {"$regularExpression" : {"pattern" : "^a", "options" : "i"}}, "$options" : "m"}});'
err --query 'db.mongo_regex_options.find({"name" : {"$regex" : "^a", "$options" : 1}});'
err --query 'db.mongo_regex_options.deleteMany({"name" : {"$not" : {"$regex" : "^a", "$options" : 1}}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, name FROM mongo_regex_options ORDER BY id"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_reserved;
    DROP TABLE mongo_size_operand;
    DROP TABLE mongo_regex_options;
"
