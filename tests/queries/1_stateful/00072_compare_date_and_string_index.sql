SELECT count() FROM test.hits WHERE EventDate = '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate < '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate > '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate <= '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate >= '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate IN ('2014-03-18', '2014-03-19');

SELECT count() FROM test.hits WHERE EventDate = toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate < toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate > toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate <= toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate >= toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate IN (toDate('2014-03-18'), toDate('2014-03-19'));

SELECT count() FROM test.hits WHERE EventDate = concat('2014-0', '3-18');

DROP TABLE IF EXISTS test.hits_indexed_by_time;
CREATE TABLE test.hits_indexed_by_time (EventDate Date, EventTime DateTime) ENGINE = MergeTree(EventDate, EventTime, 8192);
INSERT INTO test.hits_indexed_by_time SELECT EventDate, EventTime FROM test.hits;

SELECT count() FROM test.hits_indexed_by_time WHERE EventTime = '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime < '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime > '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime <= '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime >= '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime IN ('2014-03-18 01:02:03', '2014-03-19 04:05:06');

SELECT count() FROM test.hits_indexed_by_time WHERE EventTime = toDateTime('2014-03-18 01:02:03');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime < toDateTime('2014-03-18 01:02:03');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime > toDateTime('2014-03-18 01:02:03');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime <= toDateTime('2014-03-18 01:02:03');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime >= toDateTime('2014-03-18 01:02:03');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime IN (toDateTime('2014-03-18 01:02:03'), toDateTime('2014-03-19 04:05:06'));

SELECT count() FROM test.hits_indexed_by_time WHERE EventTime = concat('2014-03-18 ', '01:02:03');

DROP TABLE test.hits_indexed_by_time;
