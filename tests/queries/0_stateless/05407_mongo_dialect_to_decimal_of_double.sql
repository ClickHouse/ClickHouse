-- `$toDecimal` of a double constant converts the shortest decimal text of the double, not the
-- double itself: `toDecimal128(1.15, 2)` multiplies in floating point and truncates to `1.14`.
SET dialect = 'clickhouse';
DROP TABLE IF EXISTS to_decimal_of_double;
CREATE TABLE to_decimal_of_double (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO to_decimal_of_double VALUES (1);

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';
db.to_decimal_of_double.aggregate([{"$project" : {"a" : {"$toDecimal" : 1.15}, "b" : {"$toDecimal" : 0.29}, "c" : {"$toDecimal" : -2.675}, "d" : {"$toDecimal" : 0.00000000001}}}]);

SET dialect = 'clickhouse';
DROP TABLE to_decimal_of_double;
