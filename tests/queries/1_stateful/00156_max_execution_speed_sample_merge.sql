SET max_execution_speed = 4000000, timeout_before_checking_execution_speed = 0;

CREATE TEMPORARY TABLE times (t DateTime);

INSERT INTO times SELECT now();
SELECT count() FROM test.hits SAMPLE 1 / 2;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT count() FROM merge(test, '^hits$') SAMPLE 1 / 2;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
