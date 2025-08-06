import { Chart } from './Chart.js';
import { MergeTree } from './MergeTree.js';
import { SimulationContainer } from './SimulationContainer.js';
import { runScenario, noArrivalsScenario, SCENARIOS } from './Scenarios.js';
import { sequenceInserter } from './sequenceInserter.js';
import { customScenario } from './customScenario.js';
import { fixedBaseMerges } from './fixedBaseMerges.js';
import { floatBaseMerges } from './floatBaseMerges.js';
import { factorsAsBaseMerges } from './factorsAsBaseMerges.js';
import { simpleMerges } from './simpleMerges.js';
import { integerLayerMerges } from './integerLayerMerges.js';
import { floatLayerMerges } from './floatLayerMerges.js';
import { factorizeNumber, allFactorPermutations } from './factorization.js';
import { clickHousePartsInserter } from './clickHousePartsInserter.js';
import { delayMs } from './util.js';

// Global pages configuration including scenarios and other functions
export const PAGES = {
    // Include all scenarios
    ...Object.fromEntries(
        Object.entries(SCENARIOS).map(([key, desc]) => [key, { description: desc, type: 'scenario' }])
    ),
    // Add other functions (parallel merging)
    'periodicArrivals': { description: 'Simulation with periodic part arrivals and float layer merges (parallel merging)', type: 'function' }
};

// Tools that open separate HTML pages
export const TOOLS = {
    'solution_chart.html': { description: 'Interactive chart tool for comparing merge optimization solutions across different parameters', type: 'tool' },
    'layers_model.html': { description: 'Modeling tool for analyzing layered merge strategies with configurable bases and parameters', type: 'tool' },
    'train_solver.html': { description: 'Machine learning tool for training merge optimization solvers using gradient descent methods', type: 'tool' },
    'float_layer_merges.html': { description: 'Specialized simulation for float layer merge strategies with sequence insertion patterns', type: 'tool' }
};

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

function createNavigationPage() {
    // Hide all existing containers
    const containers = ['opt-container', 'metrics-container', 'util-container', 'rewind-container', 'time-container', 'var-container'];
    containers.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.style.display = 'none';
        }
    });

    // Create or get navigation container
    let navContainer = document.getElementById('nav-container');
    if (!navContainer) {
        navContainer = document.createElement('div');
        navContainer.id = 'nav-container';
        navContainer.className = 'container-fluid';
        document.body.insertBefore(navContainer, document.body.firstChild);
    }

    navContainer.style.display = 'block';

    // Create navigation content using Bootstrap
    navContainer.innerHTML = `
        <div class="row">
            <div class="col-12">
                <div class="jumbotron jumbotron-fluid bg-primary text-white text-center">
                    <div class="container">
                        <h1 class="display-4">ClickHouse Merge Selector Lab</h1>
                    </div>
                </div>
            </div>
        </div>
        <div class="row">
            <div class="col-12">
                <h2 class="text-success pb-2 mb-4">Single-Threaded Merging Scenarios</h2>
                <div class="row" id="scenarios-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-primary pb-2 mb-4">Parallel Merging Simulations</h2>
                <div class="row" id="functions-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-warning pb-2 mb-4">Analysis Tools</h2>
                <div class="row" id="tools-grid"></div>
            </div>
        </div>
    `;

    // Populate scenarios
    const scenariosGrid = document.getElementById('scenarios-grid');
    const functionsGrid = document.getElementById('functions-grid');
    const toolsGrid = document.getElementById('tools-grid');

    // Function to convert camelCase to Title Case
    function toTitleCase(str) {
        return str
            .replace(/_/g, ' ')  // Convert underscores to spaces (snake_case)
            .replace(/([A-Z])/g, ' $1')  // Add space before capital letters
            .replace(/([a-z])([0-9])/g, '$1 $2')  // Add space between letter and number
            .replace(/([0-9])([a-zA-Z])/g, '$1 $2')  // Add space between number and letter
            .replace(/\s+/g, ' ')  // Replace multiple spaces with single space
            .trim()  // Remove any leading/trailing spaces
            .replace(/\b\w/g, char => char.toUpperCase());  // Capitalize first letter of each word
    }

    Object.entries(PAGES).forEach(([key, page]) => {
        const card = document.createElement('div');
        card.className = 'col-lg-4 col-md-6 col-sm-12 mb-3';

        const cardColor = page.type === 'scenario' ? 'success' : 'primary';

        card.innerHTML = `
            <div class="p-3">
                <button class="btn btn-${cardColor} btn-sm mb-2 w-100">${toTitleCase(key)}</button>
                <p class="text-muted small mb-0">${page.description}</p>
            </div>
        `;

        card.querySelector('button').addEventListener('click', () => {
            window.location.hash = key;
            runPage(key);
        });

        if (page.type === 'scenario') {
            scenariosGrid.appendChild(card);
        } else {
            functionsGrid.appendChild(card);
        }
    });

    // Populate tools
    Object.entries(TOOLS).forEach(([filename, tool]) => {
        const card = document.createElement('div');
        card.className = 'col-lg-4 col-md-6 col-sm-12 mb-3';

        const toolName = filename.replace('.html', '');

        card.innerHTML = `
            <div class="p-3">
                <button class="btn btn-warning btn-sm mb-2 w-100">${toTitleCase(toolName)}</button>
                <p class="text-muted small mb-0">${tool.description}</p>
            </div>
        `;

        card.querySelector('button').addEventListener('click', () => {
            window.open(filename, '_blank');
        });

        toolsGrid.appendChild(card);
    });
}

function showContainers() {
    // Show all existing containers
    const containers = ['opt-container', 'metrics-container', 'util-container', 'rewind-container', 'time-container', 'var-container'];
    containers.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.style.display = '';
        }
    });

    // Hide navigation
    const navContainer = document.getElementById('nav-container');
    if (navContainer) {
        navContainer.style.display = 'none';
    }
}

function runPage(pageKey) {
    const page = PAGES[pageKey];
    if (!page) {
        console.error(`Unknown page: ${pageKey}`);
        return;
    }

    // Show the original containers
    showContainers();

    // Run the appropriate function
    if (page.type === 'scenario') {
        demo(pageKey);
    } else if (pageKey === 'periodicArrivals') {
        periodicArrivals();
    }
}export async function demo(scenarioName = null)
{
    const simContainer = new SimulationContainer();
    const mt = scenarioName ? runScenario(scenarioName) : runScenario();
    simContainer.update({mt}, true);
    if (mt.time > 30 * 86400)
        simContainer.rewinder.setMinTime(30 * 86400);
}

async function periodicArrivals()
{
    const simContainer = new SimulationContainer();

    const insertPartSize = 10 << 20;
    const layerBases = [Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, 3];

    const scenario = {
        inserts: [
            sequenceInserter({
                start_time: 0,
                interval: 0.05,
                parts: 10000,
                bytes: insertPartSize,
            }),
        ],
        selector: floatLayerMerges({insertPartSize, layerBases}),
        pool_size: 100,
    }

    async function updateMergeTree({mt}) {
        simContainer.update({mt}, true);
        await delayMs(1);
    }

    const signals = {
        // on_merge_begin: updateMergeTree,
        // on_merge_end: updateMergeTree,
        // on_insert: updateMergeTree,
        on_every_frame: updateMergeTree,
        on_inserter_end: ({sim}) => sim.stop(),
    };

    const mt = await customScenario(scenario, signals);
    simContainer.update({mt}, true);
}

export async function main()
{
    // Handle browser navigation
    window.addEventListener('popstate', () => {
        const hash = window.location.hash.slice(1);
        if (hash && PAGES[hash]) {
            runPage(hash);
        } else {
            createNavigationPage();
        }
    });

    // Check if there's a hash in the URL
    const hash = window.location.hash.slice(1);
    if (hash && PAGES[hash]) {
        runPage(hash);
    } else {
        createNavigationPage();
    }
}
