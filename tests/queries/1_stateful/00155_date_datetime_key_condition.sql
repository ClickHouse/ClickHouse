DROP TABLE IF EXISTS test.date_datetime_key_condition;

CREATE TABLE test.date_datetime_key_condition (dt DateTime) ENGINE = MergeTree() ORDER BY dt;
INSERT INTO test.date_datetime_key_condition VALUES ('2020-01-01 00:00:00'), ('2020-01-01 10:00:00'), ('2020-01-02 00:00:00');

-- partial
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt > toDate('2020-01-01') AND dt < toDate('2020-01-02');
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt >= toDate('2020-01-02');
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt < toDate('2020-01-02');

-- inside
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt > toDate('2019-01-02');
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt < toDate('2021-01-02');
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt >= toDate('2019-01-02') AND dt < toDate('2021-01-02');
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt > toDate('2019-01-02') OR dt <= toDate('2021-01-02');

-- outside 
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt < toDate('2019-01-02');
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt > toDate('2021-01-02');
SELECT groupArray(dt) from test.date_datetime_key_condition WHERE dt < toDate('2019-01-02') OR dt > toDate('2021-01-02');

DROP TABLE test.date_datetime_key_condition;