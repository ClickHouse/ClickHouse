import { Chart } from './Chart.js';
import { Simulator } from './Simulator.js';
import {  visualizeUtility } from './visualizeUtility.js';
import {  visualizeExecutionTime } from './visualizeExecutionTime.js';
import { runScenario, noArrivalsScenario } from './Scenarios.js';
import { fixedBaseMerges } from './fixedBaseMerges.js';
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

        const y = time_integral / total_time * (new Simulator()).mergeDuration(1 << 20, 2);
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
        let sim = noArrivalsScenario(selector, {base, parts, total_time});
        sim.title = `${selector.name} ║ Parts: ${parts}, Base: ${base} ║`;
        sim.selector = selector;
        sim.base = base;
        const time_integral = sim.integral_active_part_count;

        const y = time_integral / total_time;
        if (series)
            series.addPoint({x: base, y, sim});

        if (min_y > y)
        {
            min_y = y;
            best = sim;
        }

        await delayMs(1);
    }
    return {y: min_y, sim: best};
}

async function iteratePartsFactors(selector, series, parts, total_time = 1.0)
{
    let min_y = Infinity;
    let best = null;
    // const permutations = [...allFactorPermutations(factorizeNumber(parts)), [parts]];
    const permutations = allFactorPermutations(factorizeNumber(parts));
    console.log("ALL PERMUTATIONS", permutations);
    for (const factors of permutations)
    {
        console.log(`Permutation: ${factors.join(' x ')} = ${parts}`);

        let sim = noArrivalsScenario(selector, {factors, parts, total_time});
        sim.title = `${selector.name} ║ Parts: ${parts} = ${factors.join(' x ')} ║`;
        sim.selector = selector;
        sim.base = factors[0];
        const time_integral = sim.integral_active_part_count;

        const y = time_integral / total_time;
        if (series)
            series.addPoint({x: factors[0], y, sim});

        if (min_y > y)
        {
            min_y = y;
            best = sim;
        }

        await delayMs(1);
    }
    return {y: min_y, sim: best};
}

function showSimulation(data)
{
    console.log("SHOW", data);
    const {sim} = data;
    if (sim !== undefined)
    {
        visualizeUtility(sim, d3.select("#util-container"), true);
        visualizeExecutionTime(sim, d3.select("#exec-container"));

        // Append text with metrics
        d3.select("#metrics-container")
            .text(`
                ${sim.title === undefined ? "" : sim.title}
                Total: ${sim.written_bytes / 1024 / 1024} MB,
                Inserted: ${sim.inserted_bytes / 1024 / 1024} MB,
                WA: ${sim.writeAmplification().toFixed(2)},
                AvgPartCount: ${sim.avgActivePartCount().toFixed(2)}
                Time: ${(sim.current_time).toFixed(2)}s
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

function runSimpleMergeSelector(parts, total_time)
{
    let selector = simpleMerges;
    let sim = noArrivalsScenario(selector, {parts, total_time});
    sim.title = `${selector.name} ║ Parts: ${parts} ║`;
    sim.selector = selector;
    const time_integral = sim.integral_active_part_count;
    const y = time_integral / total_time;
    return {y: y, sim};
}

async function minimizeAvgPartCount(parts, chart)
{
    const total_time = (new Simulator()).mergeDuration(1 << 20, 2) * parts * Math.log2(parts);

    const analytical = await iterateAnalyticalSolution(chart.addSeries("Analytic"), parts, total_time);
    const futures = [
        iteratePartsFactors(factorsAsBaseMerges, chart.addSeries("FactorBases", showSimulation), parts, total_time),
        iterateBaseLinear(fixedBaseMerges, chart.addSeries("FixedBase", showSimulation), parts, total_time),
    ];

    // Simple merge selector for reference
    const simple = runSimpleMergeSelector(parts, total_time);

    // Wait all simulations to finish before we select the best
    let results = [];
    for (let future of futures)
        results.push(await future);

    // Return best solutions
    const numerical = argMin(results, d => d.y);
    return {analytical, numerical, simple};
}

export async function main()
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

    let analytical_series = optimal_chart.addSeries("Analytical", showSimulation);
    let numerical_series = optimal_chart.addSeries("Numerical", showSimulation);
    let simple_series = optimal_chart.addSeries("SimpleMergeSelector", showSimulation);
    for (let parts = 4; parts <= 120; parts++)
    {
        const {analytical, numerical, simple} = await minimizeAvgPartCount(parts, variants_chart);

        // Show what simple selector is doing
        showSimulation(simple);
        await delayMs(100);

        analytical_series.addPoint({x: parts, y: analytical.y});
        // analytical_series.addPoint({x: parts, y: analytical.base});
        numerical_series.addPoint({x: parts, y: numerical.y, sim: numerical.sim});
        // numerical_series.addPoint({x: parts, y: numerical.sim.base, sim: numerical.sim});
        simple_series.addPoint({x: parts, y: simple.y, sim: simple.sim});

        variants_chart.clear();
    }
}
