-- The resolved body tree still needs the semantic passes that follow name resolution.
SET enable_analyzer = 1;

SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT concat('SELECT ', toString(sum(g)), ' FORMAT TSV')
         FROM (SELECT grouping(number) AS g FROM numbers(1) GROUP BY number WITH ROLLUP)
         FORMAT TSVRaw));

CREATE TABLE body_final (id UInt8, version UInt8, q String)
ENGINE = ReplacingMergeTree(version) ORDER BY id;
SYSTEM STOP MERGES body_final;
INSERT INTO body_final VALUES (1, 1, 'SELECT 2 FORMAT TSV');
INSERT INTO body_final VALUES (1, 2, 'SELECT 3 FORMAT TSV');
SELECT count() FROM body_final;

-- The body inherits FINAL from the enclosing query, or specifies it in its own SETTINGS.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT q FROM body_final FORMAT TSVRaw)) SETTINGS final = 1;
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT q FROM body_final FORMAT TSVRaw SETTINGS final = 1));
DROP TABLE body_final;
