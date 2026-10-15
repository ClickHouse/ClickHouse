-- A constant pattern carries what it learned about caching per distinct haystack from one block to the next,
-- and shares it between the threads that execute the function. The per-row pattern is the reference.
-- Keep the RE2 path: the JIT-compiled matcher processes every row directly.
SET min_count_to_compile_regular_expression = 1000000000;

-- Blocks alternate between all-distinct and a cycle of three values.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(intDiv(number, 300) % 2 = 0, concat('ab', toString(number), 'cd'), concat('ab', toString(number % 3), 'cd')) AS h FROM numbers(30000))
SETTINGS max_block_size = 300;

-- Blocks shorter than a window, so no block measures anything on its own.
SELECT countIf(replaceRegexpOne(h, '^[a-z]+([0-9]+)', '<\\1>') != replaceRegexpOne(h, materialize('^[a-z]+([0-9]+)'), '<\\1>'))
FROM (SELECT if(number % 7 = 0, concat('xyz', toString(number % 5)), toString(number)) AS h FROM numbers(5000))
SETTINGS max_block_size = 10;

-- Several threads execute the same function on blocks of different shapes.
SELECT countIf(replaceRegexpAll(h, '([a-z]+)([0-9]+)', '\\2:\\1') != replaceRegexpAll(h, materialize('([a-z]+)([0-9]+)'), '\\2:\\1'))
FROM (SELECT if(intDiv(number, 1000) % 3 = 0, concat('ab', toString(number % 4), 'cd'), concat('ab', toString(number), 'cd', repeat('z', number % 50))) AS h FROM numbers_mt(200000))
SETTINGS max_block_size = 1000, max_threads = 4;
