import { Chart } from './Chart.js';
import { MergeTree } from './MergeTree.js';
import {  visualizeUtility } from './visualizeUtility.js';
import {  visualizeExecutionTime } from './visualizeExecutionTime.js';
import { runScenario, noArrivalsScenario } from './Scenarios.js';
import { customScenario } from './customScenario.js';
import { fixedBaseMerges } from './fixedBaseMerges.js';
import { floatBaseMerges } from './floatBaseMerges.js';
import { factorsAsBaseMerges } from './factorsAsBaseMerges.js';
import { simpleMerges } from './simpleMerges.js';
import { factorizeNumber, allFactorPermutations } from './factorization.js';

const delayMs = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function iterateAnalyticalSolution(series, parts, total_time = 1.0)
{
    let min_y = Infinity;
    let best_base = null;
    for (let base = 2; base <= parts / 2; base++)
    {
        const time_integral =
              (base - 3) * parts / 2 * Math.log(parts) / Math.log(base)
            + (base + 1) / (base - 1) * parts * (parts - 1) / 2
            + total_time
            ;

        const y = time_integral / total_time * (new MergeTreeSimulator()).mergeDuration(1 << 20, 2);
        series.addPoint({x: base, y});

        if (min_y > y)
        {
            min_y = y;
            best_base = base;
        }

        await delayMs(1);
    }
    return {y: min_y, base: best_base};
}

async function iterateBaseLinear(selector, series, parts, total_time = 1.0)
{
    let min_y = Infinity;
    let best = null;
    for (let base = 2; base <= parts / 2; base++)
    {
        let mt = noArrivalsScenario(selector, {base, parts, total_time});
        mt.title = `${selector.name} ║ Parts: ${parts}, Base: ${base} ║`;
        mt.selector = selector;
        mt.base = base;
        const time_integral = mt.integral_active_part_count;

        const y = time_integral / total_time;
        if (series)
            series.addPoint({x: base, y, mt});

        if (min_y > y)
        {
            min_y = y;
            best = mt;
        }

        await delayMs(1);
    }
    return {y: min_y, mt: best};
}

async function iteratePartsFactors(selector, series, parts, total_time = 1.0)
{
    let min_y = Infinity;
    let best = null;
    // const permutations = [...allFactorPermutations(factorizeNumber(parts)), [parts]];
    const permutations = allFactorPermutations(factorizeNumber(parts));
    //console.log("ALL PERMUTATIONS", permutations);
    for (const factors of permutations)
    {
        // console.log(`Permutation: ${factors.join(' x ')} = ${parts}`);
        let mt = noArrivalsScenario(selector, {factors, parts, total_time});
        mt.title = `${selector.name} ║ Parts: ${parts} = ${factors.join(' x ')} ║`;
        mt.selector = selector;
        mt.base = factors[0];
        const time_integral = mt.integral_active_part_count;

        const y = time_integral / total_time;
        if (series)
            series.addPoint({x: factors[0], y, mt});

        if (min_y > y)
        {
            min_y = y;
            best = mt;
        }

        await delayMs(1);
    }
    return {y: min_y, mt: best};
}

function showSimulation(data, automatic = false)
{
    if (automatic && window.__pause)
        return;
    if (!automatic)
        console.log("SHOW", data);
    const {mt} = data;
    if (mt !== undefined)
    {
        visualizeUtility(mt, d3.select("#util-container"), true);
        visualizeExecutionTime(mt, d3.select("#exec-container"));

        // Append text with metrics
        d3.select("#metrics-container")
            .text(`
                ${mt.title === undefined ? "" : mt.title}
                Total: ${mt.written_bytes / 1024 / 1024} MB,
                Inserted: ${mt.inserted_bytes / 1024 / 1024} MB,
                WA: ${mt.writeAmplification().toFixed(2)},
                AvgPartCount: ${mt.avgActivePartCount().toFixed(2)}
                Time: ${(mt.time).toFixed(2)}s
            `);
    }
}

function argMin(array, func)
{
    let min_value = Infinity;
    let result = null;
    for (const element of array)
    {
        const value = func(element);
        if (min_value > value)
        {
            min_value = value;
            result = element;
        }
    }
    return result;
}

function runSelector(selectorGen, opts)
{
    let mt = noArrivalsScenario(selectorGen(opts), opts);
    const { count, total_time, ...other_opts } = opts;
    mt.title = `${selectorGen.name} ║ ${JSON.stringify(other_opts)} ║`;
    mt.selectorGen = selectorGen;
    const time_integral = mt.integral_active_part_count;
    const y = time_integral / total_time;
    return {y: y, mt};
}

async function minimizeAvgPartCount(parts, chart)
{
    const total_time = (new MergeTree()).mergeDuration(1 << 20, 2) * parts * Math.log2(parts);

    let results = {};

    // results.analytical = await iterateAnalyticalSolution(chart.addSeries("Analytic"), parts, total_time);
    const numerical_futures = [
        //iteratePartsFactors(factorsAsBaseMerges, chart.addSeries("FactorBases", showSimulation), parts, total_time),
        // iterateBaseLinear(fixedBaseMerges, chart.addSeries("FixedBase", showSimulation), parts, total_time),
    ];

    results.base_2 = runSelector(floatBaseMerges, {parts, total_time, base: 2});
    results.base_2_5 = runSelector(floatBaseMerges, {parts, total_time, base: 2.5});
    results.base_e = runSelector(floatBaseMerges, {parts, total_time, base: Math.E});
    results.base_3 = runSelector(floatBaseMerges, {parts, total_time, base: 3});
    results.base_3_5 = runSelector(floatBaseMerges, {parts, total_time, base: 3.5});

    // Simple merge selector for reference
    results.simple = runSelector(simpleMerges, {parts, total_time});

    // Wait all simulations to finish before we select the best
    let numerical_results = [];
    for (let future of numerical_futures)
        numerical_results.push(await future);

    // Return best solutions
    // results.numerical = argMin(numerical_results, d => d.y);
    return results;
}

export async function demo()
{
    showSimulation({mt: runScenario()}, true);
}

export async function compareAvgPartCount()
{
    const optimal_chart = new Chart(
        d3.select("#opt-container"),
        "Initial number of parts to merge",
        "Avg Part Count",
        `This chart shows how different <i>Merge Selectors</i> are compared according to average count of parts over time.
        For fair comparison the same time interval is selected for all simulations.
        So basically we compare time integral of part count against initial number of parts.
        <br><b>Click a point to see corresponding tree of merges.</b>`
    );

    const variants_chart = new Chart(
        d3.select("#var-container"),
        "Base (Part count for 1st level merges)",
        "Avg Part Count",
        `This chart shows results of multiple simulations with <i>numerical algorithms</i> that explore possible merge tree structures.
        Every simulation has different <i>parameters</i>.
        This is done in attempt to find the best merging approach in given scenario.
        Best (lowest) point is then returned as a <u>numerical</u> result and is shown on another chart.
        It represents best guess we could hope for given optimization goal: minimize avg part count.
        <br><b>Click a point to see corresponding tree of merges.</b>`
    );
    variants_chart.trackMin();

    // let analytical_series = optimal_chart.addSeries("Analytical", showSimulation);
    // let numerical_series = optimal_chart.addSeries("Numerical", showSimulation);
    // let simple_series = optimal_chart.addSeries("SimpleMergeSelector", showSimulation);
    let series = {};
    for (let parts = 100; parts <= 1000; parts+=100)
    {
        const results = await minimizeAvgPartCount(parts, variants_chart);
        for (let name in results)
        {
            if (!(name in series))
                series[name] = optimal_chart.addSeries(name, showSimulation);
            series[name].addPoint({x: parts, ...results[name]});
        }
        await delayMs(10);

        // Show what simple selector is doing
        // showSimulation(simple, true);
        //await delayMs(100);

        // analytical_series.addPoint({x: parts, y: analytical.y});
        // analytical_series.addPoint({x: parts, y: analytical.base});
        // numerical_series.addPoint({x: parts, y: numerical.y, mt: numerical.mt});
        // numerical_series.addPoint({x: parts, y: numerical.mt.base, mt: numerical.mt});
        // simple_series.addPoint({x: parts, y: simple.y, mt: simple.mt});

        variants_chart.clear();
    }
}

function* sequenceInserter({start_time, interval, parts, bytes})
{
    yield {type: 'sleep', delay: start_time};
    for (let i = 0; i < parts; i++)
    {
        yield {type: 'insert', bytes};
        yield {type: 'sleep', delay: interval};
    }
}

async function custom()
{
    const scenario = {
        inserts: [
            sequenceInserter({
                start_time: 0,
                interval: 0,
                parts: 5000,
                bytes: 10 << 20,
            }),
        ],
        selector: simpleMerges(),
        pool_size: 1000,
    }
    const mt = await customScenario(scenario);
    showSimulation({mt}, true);
}

export async function main()
{
    // demo();
    // compareAvgPartCount();
    custom();
}
