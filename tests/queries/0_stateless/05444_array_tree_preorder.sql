-- Input in arbitrary order.
SELECT arrayTreePreorder(['G2', 'A', 'T1', 'B', 'G1', 'G3'], ['B', '', 'B', 'A', 'A', 'A']);
-- Children are visited in the input order.
SELECT arrayTreePreorder(['r', 'a', 'b', 'c'], ['', 'r', 'r', 'a']);

-- Forest: orphans are roots, the trees go in the input order of their roots.
SELECT arrayTreePreorder(['a', 'b', 'c', 'd', 'e'], ['', 'x', 'a', 'b', 'y']);

-- Cycle C -> D -> E -> C, F is below C, G is an orphan.
SELECT arrayTreePreorder(['A', 'B', 'C', 'D', 'E', 'F', 'G'], ['', 'A', 'D', 'E', 'C', 'C', 'Z']);
-- Self-loops: a root marked by its own id.
SELECT arrayTreePreorder(['a', 'b', 'c'], ['a', 'a', 'c']);
-- The whole array is one cycle. It is cut at the first input node.
SELECT arrayTreePreorder([3, 1, 2], [1, 2, 3]);
-- Two cycles and a node below both.
SELECT arrayTreePreorder(['x', 'p', 'q', 'y', 'r', 's'], ['p', 'q', 'p', 'r', 's', 'r']);

-- Duplicate ids: children are attached to the first occurrence.
SELECT arrayTreePreorder(['a', 'b', 'a', 'c'], ['', 'a', '', 'a']);

-- Element types: numbers, FixedString, Nullable, a generic type, compatible types, LowCardinality.
SELECT arrayTreePreorder([10, 20, 30], [0, 10, 20]);
SELECT arrayTreePreorder(CAST(['a', 'b', 'c'], 'Array(FixedString(1))'), CAST(['', 'a', 'b'], 'Array(FixedString(1))'));
SELECT arrayTreePreorder([1, NULL, 3], [NULL, 1, NULL]);
SELECT arrayTreePreorder([NULL, 1], [1, NULL]);
SELECT arrayTreePreorder([(1, 'a'), (2, 'b')], [(0, ''), (1, 'a')]);
SELECT arrayTreePreorder([1::UInt8, 2, 3], [0::Int64, 1, 2]);
SELECT arrayTreePreorder(CAST(['A', 'B'], 'Array(LowCardinality(String))'), ['', 'A']);
SELECT toTypeName(arrayTreePreorder(['a'], ['']));

-- Empty arrays, constant arguments, several rows.
SELECT arrayTreePreorder(CAST([], 'Array(String)'), CAST([], 'Array(String)'));
SELECT arrayTreePreorder(ids, parents) FROM values('ids Array(String), parents Array(String)', (['a', 'b'], ['', 'a']), ([], []), (['x', 'y', 'z'], ['z', 'x', '']), (['p'], ['p']));
SELECT arrayTreePreorder(ids, ['', 'a']) FROM values('ids Array(String)', ['a', 'b'], ['b', 'a']);

-- Deep chain, deeper than 64, and a wide star.
SELECT t.order = range(1, 1001), t.size = arrayReverse(range(1, 1001)), t.parent = range(1000), t.depth = range(1000)
FROM (SELECT arrayTreePreorder(range(1, 1001), range(1000)) AS t);
SELECT t.order = range(1, 1001), t.size[1], arrayAll(x -> x = 1, arraySlice(t.size, 2)), countEqual(t.parent, 1), arraySum(t.depth)
FROM (SELECT arrayTreePreorder(range(1000), arrayMap(x -> if(x = 0, 1000, 0), range(1000))) AS t);
-- A long cycle.
SELECT t.order[1], t.size[1], t.parent[1], t.depth[1000]
FROM (SELECT arrayTreePreorder(range(1000), arrayMap(x -> (x + 999) % 1000, range(1000))) AS t);

SELECT arrayTreePreorder(['a', 'b'], ['a']); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT arrayTreePreorder(['a'], [1]); -- { serverError NO_COMMON_TYPE }
SELECT arrayTreePreorder('a', ['a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder(['a']); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Floating-point numbers, Dynamic, Variant and JSON are not supported as identifiers, also inside other types.
SELECT arrayTreePreorder([0.0, 1.0], [2.0, -0.0]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder([1, 2], [0.0, 1.0]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder([toBFloat16(1)], [toBFloat16(0)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder([(1, 0.5)], [(0, 0.5)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder([[1.5]], [[0.5]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder(CAST([1, NULL], 'Array(Nullable(Float32))'), CAST([NULL, 1], 'Array(Nullable(Float32))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder([map('a', 1.5)], [map('a', 0.5)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder(CAST([NULL, 'a'], 'Array(Dynamic)'), CAST(['a', NULL], 'Array(Dynamic)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder(CAST([NULL, 'a'], 'Array(Variant(String, UInt64))'), CAST(['a', NULL], 'Array(Variant(String, UInt64))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder(['a'], CAST([NULL], 'Array(Dynamic)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTreePreorder(CAST(['{"a": 1}'], 'Array(JSON)'), CAST(['{}'], 'Array(JSON)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Decimal and LowCardinality(Nullable) are fine; a NULL of LowCardinality(Nullable) makes a root.
SELECT arrayTreePreorder(CAST(['1.5', '2.5'], 'Array(Decimal(10, 2))'), CAST(['0', '1.5'], 'Array(Decimal(10, 2))'));
SELECT arrayTreePreorder(CAST([NULL, 'a'], 'Array(LowCardinality(Nullable(String)))'), CAST(['a', NULL], 'Array(LowCardinality(Nullable(String)))'));

-- Sum of cost and count of strict descendants for each agent span.
DROP TABLE IF EXISTS nodes;
CREATE TABLE nodes (trace_id String, id String, parent_id String, type String, cost Float64) ENGINE = Memory;
INSERT INTO nodes VALUES
    ('t1', 'A', '', 'AGENT', 0), ('t1', 'G1', 'A', 'GENERATION', 2), ('t1', 'B', 'A', 'AGENT', 0),
    ('t1', 'G2', 'B', 'GENERATION', 3), ('t1', 'T1', 'B', 'TOOL', 0), ('t1', 'G3', 'A', 'GENERATION', 1),
    -- An orphan agent with a child and an agent without descendants.
    ('t2', 'C', 'missing', 'AGENT', 0), ('t2', 'G4', 'C', 'GENERATION', 5), ('t2', 'D', '', 'AGENT', 0);

SELECT trace_id, ids[order[k]] AS agent_id, cum[k + size[k] - 1] - cum[k] AS subtree_cost, size[k] - 1 AS span_count
FROM
(
    SELECT
        trace_id,
        groupArray(id) AS ids,
        groupArray(type) AS types,
        groupArray(cost) AS costs,
        arrayTreePreorder(ids, groupArray(parent_id)) AS t,
        t.order AS order,
        t.size AS size,
        arrayCumSum(arrayMap(i -> costs[i], order)) AS cum,
        arrayFilter(k -> types[order[k]] = 'AGENT' AND size[k] > 1, arrayEnumerate(order)) AS agents
    FROM nodes
    GROUP BY trace_id
)
ARRAY JOIN agents AS k
ORDER BY trace_id, agent_id;

DROP TABLE nodes;
