import { MergeTree } from './MergeTree.js';
import { simpleMerges } from './simpleMerges.js';
import { maxWaMerges } from './maxWaMerges.js';
import { fixedBaseMerges } from './fixedBaseMerges.js';

////////////////////////////////////////////////////////////////////////////////
// Scenario helpers
//
function rnd(a, b) { return a + Math.random() * (b - a); }

function randomInserts(mt, {count, min_size, max_size, now})
{
    for (let i = 0; i < count; i++)
        mt.insertPart(rnd(min_size, max_size) << 20, now);
}

function randomMerges(mt, {count, min_parts, max_parts})
{
    for (let i = 0; i < count; i++)
    {
        const active_parts = mt.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
        const part_count = rnd(min_parts, max_parts);
        if (active_parts.length < part_count)
            break;
        const begin = Math.floor(Math.random() * (active_parts.length - part_count));
        const end = begin + part_count;
        mt.mergeParts(active_parts.slice(begin, end));
    }
}

////////////////////////////////////////////////////////////////////////////////
// Scenarios
//
function explainDemo()
{
    const mt = new MergeTree();
    randomInserts(mt, {count: 20, min_size: 10, max_size: 100});
    runSelector(mt, 7, maxWaMerges({min_parts: 2, max_parts: 5, min_score: 1.5}));
    return mt;
}

function oneBigMerge()
{
    const mt = new MergeTree();
    randomInserts(mt, {count: 16, min_size: 10, max_size: 100});
    mt.mergeParts(mt.parts.filter(d => d.active));
    return mt;
}

function aggressiveMerging()
{
    const mt = new MergeTree();
    mt.insertPart(1 << 20);
    for (let i = 0; i < 15; i++)
    {
        mt.insertPart(1 << 20);
        mt.mergeParts(mt.parts.filter(d => d.active));
    }
    return mt;
}

function binaryTree()
{
    const mt = new MergeTree();
    for (let i = 0; i < 16; i++)
        mt.insertPart(1 << 20);
    let i = mt.parts.length;
    while (i >= 2)
    {
        let min_size = d3.min(mt.parts.filter(d => d.active), d => d.bytes);
        console.log("MIN SIZE", min_size);
        mt.mergeParts(mt.parts.filter(d => d.active && d.bytes == min_size).slice(0,2));
        i--;
    }
    return mt;
}

function randomMess()
{
    const mt = new MergeTree();
    randomInserts(mt, {count: 100, min_size: 10, max_size: 100});
    randomMerges(mt, {count: 30, min_parts: 2, max_parts: 6});
    return mt;
}

function maxWaDemo()
{
    const mt = new MergeTree();
    for (let i = 0; i < 300; i++)
    {
        randomInserts(mt, {count: 10, min_size: 10, max_size: 100});
        runSelector(mt, 3, maxWaMerges({min_parts: 2, max_parts: 20, min_score: Math.log2(5)}));
    }
    return mt;
}

function simple1000()
{
    const mt = new MergeTree();
    for (let i = 0; i < 1; i++)
    {
        randomInserts(mt, {count: 1000, min_size: 10, max_size: 100});
        mt.advanceTime(30 * 86400);
        runSelector(mt, 1000, simpleMerges());
    }
    return mt;
}

function maxWa1000()
{
    const mt = new MergeTree();
    const max_wa = 5;
    for (let i = 0; i < 1; i++)
    {
        randomInserts(mt, {count: 1000, min_size: 10, max_size: 100, now: 0});
        const current_size = d3.sum(mt.parts.filter(d => d.active), d => d.bytes);
        const inserts = mt.parts.filter(d => d.level == 0);
        const avg_insert_size = d3.sum(inserts, d => d.bytes) / inserts.length;
        const min_score = Math.log2(current_size/avg_insert_size) / (max_wa - 1);
        runSelector(mt, 10, maxWaMerges({min_parts: 2, max_parts: 100, min_score}));
    }
    return mt;
}

function simple10000Period()
{
    const mt = new MergeTree();
    let dt = 0.1;
    for (let i = 0; i < 1000; i++)
    {
        randomInserts(mt, {count: 10, min_size: 1, max_size: 100});
        mt.advanceTime(mt.time + dt);
        runSelector(mt, 10, simpleMerges());
        mt.advanceTime(mt.time + dt);
    }
    return mt;
}

function maxWa10000Period()
{
    const mt = new MergeTree();
    const max_wa = 5;
    for (let i = 0; i < 1000; i++)
    {
        randomInserts(mt, {count: 10, min_size: 1, max_size: 100});
        const current_size = d3.sum(mt.parts.filter(d => d.active), d => d.bytes);
        const inserts = mt.parts.filter(d => d.level == 0);
        const avg_insert_size = d3.sum(inserts, d => d.bytes) / inserts.length;
        const min_score = Math.log2(current_size/avg_insert_size) / (max_wa - 1);
        runSelector(mt, 10, maxWaMerges({min_parts: 2, max_parts: 100, min_score}));
    }
    return mt;
}

function runSelector(mt, count, selector)
{
    let value_to_send = null;
    loop: while (true)
    {
        const { value, done } = selector.next(value_to_send);
        value_to_send = null;
        if (done)
            return; // No more merges required
        switch (value.type)
        {
            case 'getMergeTree':
                value_to_send = mt;
                break;
            case 'merge':
                mt.mergeParts(value.parts_to_merge);
                if (--count == 0)
                    return;
                break;
            case 'wait':
                return;
            default:
                throw { message: "Unknown merge selector yield type", value};
        }
    }
}

export function noArrivalsScenario(selector, opts)
{
    const mt = new MergeTree();
    const {parts, total_time} = opts;

    randomInserts(mt, {count: parts, min_size: 1, max_size: 1});
    runSelector(mt, mt.active_part_count, selector);
    if (mt.time < total_time)
        mt.advanceTime(total_time);
    return mt;
}

////////////////////////////////////////////////////////////////////////////////

export const SCENARIOS = {
    'explainDemo': 'Small demo with 20 parts using maxWa merge strategy with 7 merges',
    'binaryTree': 'Creates 16 equal-sized parts and merges them in a binary tree pattern',
    'aggressiveMerging': 'Demonstrates aggressive merging by merging all parts after each insert',
    'randomMess': 'Creates chaotic state with 100 random inserts followed by 30 random merges',
    'maxWaDemo': 'Long-running demo with 300 iterations of inserts and maxWa merges',
    'simple1000': 'Inserts 1000 parts and applies simple merge strategy',
    'maxWa1000': 'Inserts 1000 parts and applies maxWa merge strategy with adaptive scoring',
    'oneBigMerge': 'Creates 16 parts and merges them all into one large part',
    'simple10000Period': 'Periodic scenario with 1000 iterations of 10 inserts and simple merges',
    'maxWa10000Period': 'Periodic scenario with 1000 iterations of 10 inserts and maxWa merges',
    'noArrivalsScenario': 'Fixed base merge scenario with 256 parts and base factor of 4'
};

export function runScenario(scenarioName = 'simple1000')
{
    switch (scenarioName) {
        case 'explainDemo':
            return explainDemo();
        case 'binaryTree':
            return binaryTree();
        case 'aggressiveMerging':
            return aggressiveMerging();
        case 'randomMess':
            return randomMess();
        case 'maxWaDemo':
            return maxWaDemo();
        case 'simple1000':
            return simple1000();
        case 'maxWa1000':
            return maxWa1000();
        case 'oneBigMerge':
            return oneBigMerge();
        case 'simple10000Period':
            return simple10000Period();
        case 'maxWa10000Period':
            return maxWa10000Period();
        case 'noArrivalsScenario':
            return noArrivalsScenario(fixedBaseMerges, {base: 4, parts: 256});
        default:
            throw new Error(`Unknown scenario: ${scenarioName}. Available scenarios: ${Object.keys(SCENARIOS).join(', ')}`);
    }
}
