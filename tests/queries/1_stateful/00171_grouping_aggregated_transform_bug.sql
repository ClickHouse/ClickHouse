-- Tags: distributed

SET max_rows_to_read = '100M';
SELECT sum(cityHash64(*)) FROM (SELECT CounterID, quantileTiming(0.5)(SendTiming), count() FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', test.hits) WHERE SendTiming != -1 GROUP BY CounterID) SETTINGS max_block_size = 63169;
SELECT sum(cityHash64(*)) FROM (SELECT CounterID, quantileTiming(0.5)(SendTiming), count() FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', test.hits) WHERE SendTiming != -1 GROUP BY CounterID) SETTINGS optimize_aggregation_in_order = 1, max_block_size = 63169;
