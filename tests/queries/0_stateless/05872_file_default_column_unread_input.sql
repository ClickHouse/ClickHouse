-- Tags: no-fasttest
INSERT INTO FUNCTION file('05872_{_partition_id}.jsonl', 'JSONEachRow', 'external UInt32') PARTITION BY 1 VALUES (1);
SELECT id, next FROM file('05872_1.jsonl', 'JSONEachRow', 'id UInt32 DEFAULT 5, next UInt32 DEFAULT id + 1');
SELECT next FROM file('05872_1.jsonl', 'JSONEachRow', 'id UInt32 DEFAULT 5, next UInt32 DEFAULT id + 1');
SELECT last FROM file('05872_1.jsonl', 'JSONEachRow', 'id UInt32 DEFAULT 5, next UInt32 DEFAULT id + 1, last UInt32 DEFAULT next * 2');
