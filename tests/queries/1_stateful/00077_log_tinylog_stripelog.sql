SET check_query_single_value_result = 1;
SET allow_table_engine_log=1, allow_table_engine_tinylog=1, allow_table_engine_stripelog=1;

DROP TABLE IF EXISTS test.hits_log;
DROP TABLE IF EXISTS test.hits_tinylog;
DROP TABLE IF EXISTS test.hits_stripelog;

CREATE TABLE test.hits_log (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID UInt64) ENGINE = Log;
CREATE TABLE test.hits_tinylog (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID UInt64) ENGINE = TinyLog;
CREATE TABLE test.hits_stripelog (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID UInt64) ENGINE = StripeLog;

CHECK TABLE test.hits_log;
CHECK TABLE test.hits_tinylog;
CHECK TABLE test.hits_stripelog;

INSERT INTO test.hits_log SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID FROM test.hits;
INSERT INTO test.hits_tinylog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID FROM test.hits;
INSERT INTO test.hits_stripelog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID FROM test.hits;

SELECT count(), sum(cityHash64(CounterID, AdvEngineID, RegionID, SearchPhrase, UserID)) FROM test.hits;
SELECT count(), sum(cityHash64(*)) FROM test.hits_log;
SELECT count(), sum(cityHash64(*)) FROM test.hits_tinylog;
SELECT count(), sum(cityHash64(*)) FROM test.hits_stripelog;

CHECK TABLE test.hits_log;
CHECK TABLE test.hits_tinylog;
CHECK TABLE test.hits_stripelog;

DROP TABLE test.hits_log;
DROP TABLE test.hits_tinylog;
DROP TABLE test.hits_stripelog;
