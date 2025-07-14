-- Test cases for LIMIT BY ALL functionality

-- Create test table
CREATE TABLE test_limit_by_all (
    id Int32,
    category String,
    value Int32,
    name String
) ENGINE = Memory;

INSERT INTO test_limit_by_all VALUES 
(1, 'A', 100, 'item1'),
(1, 'A', 200, 'item2'),
(1, 'B', 300, 'item3'),
(2, 'A', 400, 'item4'),
(2, 'A', 500, 'item5'),
(2, 'B', 600, 'item6'),
(3, 'C', 700, 'item7');

-- Test 1: Basic LIMIT BY ALL
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 2: LIMIT BY ALL with OFFSET
-- Should expand to LIMIT 1 OFFSET 1 BY id, category, value
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 OFFSET 1 BY ALL;

-- Test 3: LIMIT BY ALL with mixed expressions
-- Should extract non-aggregate expressions
SELECT id, category, concat(category, '_', name) as combined
FROM test_limit_by_all 
ORDER BY id, category 
LIMIT 2 BY ALL;

-- Test 4: LIMIT BY ALL with aggregate functions
-- Should only use non-aggregate expressions from SELECT
SELECT id, category, count(*) as cnt
FROM test_limit_by_all 
GROUP BY id, category
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 5: Complex expression with both aggregate and non-aggregate parts
-- Should extract maximum non-aggregate fields
SELECT 
    substring(category, 1, 1) as first_char,
    substring(concat(category, toString(count(*))), 1, 2) as mixed_expr
FROM test_limit_by_all 
GROUP BY category 
LIMIT 1 BY ALL;

-- Test 6: Verify it's equivalent to explicit column list
-- These two queries should return the same result:
SELECT id, category FROM test_limit_by_all ORDER BY id, category LIMIT 1 BY ALL;
SELECT id, category FROM test_limit_by_all ORDER BY id, category LIMIT 1 BY id, category;

-- Test 7: With WHERE clause
SELECT id, category, value 
FROM test_limit_by_all 
WHERE value > 200
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 8: Combined with regular LIMIT
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL
LIMIT 5;

DROP TABLE test_limit_by_all; 