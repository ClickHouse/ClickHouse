SET output_format_write_statistics = 0;
SELECT EventDate, count() FROM remote('127.0.0.1', test.hits) WHERE UserID GLOBAL IN (SELECT UserID FROM test.hits) GROUP BY EventDate ORDER BY EventDate LIMIT 5 FORMAT JSONCompact;
