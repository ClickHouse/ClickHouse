-- Each update of a key appends a new accumulated row and marks the previous one dead. Dead rows are
-- reclaimed as soon as they are at least half of the accumulated rows, however few rows there are, so a
-- single key updated on every step does not retain every previous payload (about 100 MB here without the
-- compaction, against a 64 MiB limit).

SET allow_experimental_keyed_recursive_cte = 1;

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, toUInt64(0) AS iteration, repeat('x', 1000000) AS payload
    UNION ALL
    SELECT k, iteration + 1, payload FROM t WHERE iteration < 100
)
SELECT max(iteration), max(length(payload)) FROM t
SETTINGS max_threads = 1, max_memory_usage = 67108864;
