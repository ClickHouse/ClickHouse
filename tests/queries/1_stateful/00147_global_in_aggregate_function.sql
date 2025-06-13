-- Tags: global

SELECT sum(UserID GLOBAL IN (SELECT UserID FROM remote('127.0.0.{1,2}', test.hits))) FROM remote('127.0.0.{1,2}', test.hits);
SELECT sum(UserID GLOBAL IN (SELECT UserID FROM test.hits)) FROM remote('127.0.0.{1,2}', test.hits);
