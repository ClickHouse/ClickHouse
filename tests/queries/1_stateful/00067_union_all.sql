SELECT * FROM
(
	SELECT UserID AS id, 1 AS event
	FROM remote('127.0.0.{1,2}', test, hits)
	ORDER BY id DESC
	LIMIT 10
UNION ALL
	SELECT FUniqID AS id, 2 AS event
	FROM remote('127.0.0.{1,2}', test, hits)
	ORDER BY id DESC
	LIMIT 10
)
ORDER BY id, event;
