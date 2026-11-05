-- A field whose first value is `null` is stored as `Dynamic`, and its later values are of any
-- type. `$toDate` reads a number held by such a field as Unix milliseconds, a string as a date,
-- keeps a date, and converts a `null` to `null` - the type is the one of the value in each row,
-- not the constant type of the column. `$toLong` and `$toDouble` of a date held by such a field
-- are its epoch milliseconds, and `$toString` is its ISO 8601 text in UTC. Both analyzers are
-- covered, the old one also without short-circuit evaluation, where both branches of the dispatch
-- run.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect = 'clickhouse';

DROP TABLE IF EXISTS to_date_dynamic;
CREATE TABLE to_date_dynamic (id Int32, v Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO to_date_dynamic SELECT 1, NULL;
INSERT INTO to_date_dynamic SELECT 2, 1500::Int64;
INSERT INTO to_date_dynamic SELECT 3, '2019-01-01 00:00:00'::String;
INSERT INTO to_date_dynamic SELECT 4, 1546300800000::Int64;
INSERT INTO to_date_dynamic SELECT 5, 1500.7::Float64;
INSERT INTO to_date_dynamic SELECT 6, toDateTime64('2020-01-01 00:00:00', 3, 'UTC');

SET enable_mongo_dialect = 1;
SET dialect = 'mongo';

db.to_date_dynamic.aggregate([{"$project" : {"_id" : 0, "id" : 1, "t" : {"$toDate" : "$v"}}}, {"$sort" : {"id" : 1}}]);

db.to_date_dynamic.find({"$expr" : {"$eq" : [{"$toDate" : "$v"}, {"$toDate" : 1546300800000}]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});

db.to_date_dynamic.aggregate([{"$match" : {"id" : {"$in" : [2, 4, 6]}}}, {"$project" : {"_id" : 0, "id" : 1, "l" : {"$toLong" : "$v"}, "d" : {"$toDouble" : "$v"}, "s" : {"$toString" : "$v"}}}, {"$sort" : {"id" : 1}}]);

SET enable_analyzer = 0;
SET short_circuit_function_evaluation = 'disable';

db.to_date_dynamic.aggregate([{"$project" : {"_id" : 0, "id" : 1, "t" : {"$toDate" : "$v"}}}, {"$sort" : {"id" : 1}}]);

db.to_date_dynamic.aggregate([{"$match" : {"id" : {"$in" : [2, 4, 6]}}}, {"$project" : {"_id" : 0, "id" : 1, "l" : {"$toLong" : "$v"}, "d" : {"$toDouble" : "$v"}, "s" : {"$toString" : "$v"}}}, {"$sort" : {"id" : 1}}]);

SET dialect = 'clickhouse';
DROP TABLE to_date_dynamic;
