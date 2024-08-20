-- Tags: global

SET max_rows_to_read = 100_000_000;
SELECT sum(UserID GLOBAL IN (SELECT UserID FROM remote('127.0.0.{1,2}', test.hits))) FROM remote('127.0.0.{1,2}', test.hits);
SELECT sum(UserID GLOBAL IN (SELECT UserID FROM test.hits)) FROM remote('127.0.0.{1,2}', test.hits);
