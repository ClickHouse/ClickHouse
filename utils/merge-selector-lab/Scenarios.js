import { Simulator } from './Simulator.js';
import { simpleMerges } from './simpleMerges.js';
import { maxWaMerges } from './maxWaMerges.js';
import { fixedBaseMerges } from './fixedBaseMerges.js';

////////////////////////////////////////////////////////////////////////////////
// Scenario helpers
//
function rnd(a, b) { return a + Math.random() * (b - a); }

function randomInserts(sim, {count, min_size, max_size, now})
{
    for (let i = 0; i < count; i++)
        sim.insertPart(rnd(min_size, max_size) << 20, now);
}

function randomMerges(sim, {count, min_parts, max_parts})
{
    for (let i = 0; i < count; i++)
    {
        const active_parts = sim.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
        const part_count = rnd(min_parts, max_parts);
        if (active_parts.length < part_count)
            break;
        const begin = Math.floor(Math.random() * (active_parts.length - part_count));
        const end = begin + part_count;
        sim.mergeParts(active_parts.slice(begin, end));
    }
}

////////////////////////////////////////////////////////////////////////////////
// Scenarios
//
function explainDemo()
{
    const sim = new Simulator();
    randomInserts(sim, {count: 20, min_size: 10, max_size: 100});
    maxWaMerges(sim, {count: 7, min_parts: 2, max_parts: 5, min_score: 1.5});
    return sim;
}

function oneBigMerge()
{
    const sim = new Simulator();
    randomInserts(sim, {count: 16, min_size: 10, max_size: 100});
    sim.mergeParts(sim.parts.filter(d => d.active));
    return sim;
}

function aggressiveMerging()
{
    const sim = new Simulator();
    sim.insertPart(1 << 20);
    for (let i = 0; i < 15; i++)
    {
        sim.insertPart(1 << 20);
        sim.mergeParts(sim.parts.filter(d => d.active));
    }
    return sim;
}

function binaryTree()
{
    const sim = new Simulator();
    for (let i = 0; i < 16; i++)
        sim.insertPart(1 << 20);
    let i = sim.parts.length;
    while (i >= 2)
    {
        let min_size = d3.min(sim.parts.filter(d => d.active), d => d.bytes);
        console.log("MIN SIZE", min_size);
        sim.mergeParts(sim.parts.filter(d => d.active && d.bytes == min_size).slice(0,2));
        i--;
    }
    return sim;
}

function randomMess()
{
    const sim = new Simulator();
    randomInserts(sim, {count: 100, min_size: 10, max_size: 100});
    randomMerges(sim, {count: 30, min_parts: 2, max_parts: 6});
    return sim;
}

function maxWaDemo()
{
    const sim = new Simulator();
    for (let i = 0; i < 300; i++)
    {
        randomInserts(sim, {count: 10, min_size: 10, max_size: 100});
        maxWaMerges(sim, {count: 3, min_parts: 2, max_parts: 20, min_score: Math.log2(5)});
    }
    return sim;
}

function simple1000()
{
    const sim = new Simulator();
    for (let i = 0; i < 1; i++)
    {
        randomInserts(sim, {count: 1000, min_size: 10, max_size: 100});
        //sim.advanceTime(30 * 86400);
        simpleMerges(sim, {count: 1000});
    }
    return sim;
}

function maxWa1000()
{
    const sim = new Simulator();
    const max_wa = 5;
    for (let i = 0; i < 1; i++)
    {
        randomInserts(sim, {count: 1000, min_size: 10, max_size: 100, now: 0});
        const current_size = d3.sum(sim.parts.filter(d => d.active), d => d.bytes);
        const inserts = sim.parts.filter(d => d.level == 0);
        const avg_insert_size = d3.sum(inserts, d => d.bytes) / inserts.length;
        const min_score = Math.log2(current_size/avg_insert_size) / (max_wa - 1);
        maxWaMerges(sim, {count: 10, min_parts: 2, max_parts: 100, min_score});
    }
    return sim;
}

function simple10000Period()
{
    const sim = new Simulator();
    let dt = 0.1;
    for (let i = 0; i < 1000; i++)
    {
        randomInserts(sim, {count: 10, min_size: 1, max_size: 100});
        sim.advanceTime(sim.current_time + dt);
        simpleMerges(sim, {count: 10});
        sim.advanceTime(sim.current_time + dt);
    }
    return sim;
}

function maxWa10000Period()
{
    const sim = new Simulator();
    const max_wa = 5;
    for (let i = 0; i < 1000; i++)
    {
        randomInserts(sim, {count: 10, min_size: 1, max_size: 100});
        const current_size = d3.sum(sim.parts.filter(d => d.active), d => d.bytes);
        const inserts = sim.parts.filter(d => d.level == 0);
        const avg_insert_size = d3.sum(inserts, d => d.bytes) / inserts.length;
        const min_score = Math.log2(current_size/avg_insert_size) / (max_wa - 1);
        maxWaMerges(sim, {count: 10, min_parts: 2, max_parts: 100, min_score});
    }
    return sim;
}

export function noArrivalsScenario(selector, opts)
{
    const sim = new Simulator();
    const {parts, total_time} = opts;

    randomInserts(sim, {count: parts, min_size: 1, max_size: 1});
    opts.count = sim.active_part_count;
    selector(sim, opts);
    if (sim.current_time < total_time)
        sim.advanceTime(total_time);
    return sim;
}

////////////////////////////////////////////////////////////////////////////////

export function runScenario()
{
    // return explainDemo();
    // return binaryTree();
    // return aggressiveMerging();
    // return randomMess();
    // return maxWaDemo();
    // return simple1000();
    // return maxWa1000();
    // return oneBigMerge();
    // return simple10000Period();
    // return maxWa10000Period();
    return noArrivalsScenario(fixedBaseMerges, {base: 4, parts: 256});
}
