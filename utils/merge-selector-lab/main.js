import { SimulationContainer } from './SimulationContainer.js';
import { runScenario } from './Scenarios.js';
import { sequenceInserter } from './sequenceInserter.js';
import { customScenario } from './customScenario.js';
import { floatLayerMerges } from './floatLayerMerges.js';
import { delayMs } from './util.js';

// Global pages configuration including scenarios and other functions
const PAGES = {
    // Tutorial scenarios
    'explainVisualizations': { description: 'Small demo with 20 initial parts and a few merges', type: 'tutorial', color: 'info' },
    'oneBigMerge': { description: 'Creates 16 equal-sized parts and merges them all into one large part', type: 'tutorial', color: 'info' },
    'aggressiveMerging': { description: 'Creates 16 equal-sized parts and aggressive merge all parts after each insert.', type: 'tutorial', color: 'info' },
    'binaryTree': { description: 'Creates 16 equal-sized parts and merges them in a binary tree pattern. Note that this scenario does sequential merging, while the tree allows for parallelization. Note the grey area in the time diagram.', type: 'tutorial', color: 'info' },
    'randomMess': { description: 'Creates chaotic state with 100 random inserts followed by 30 random merges', type: 'tutorial', color: 'info' },

    // Sequential Simple Selector scenarios
    'simpleMergesDemo': { description: 'Inserts 1000 parts and applies simple merge strategy. Simple merge selector uses age to restrict merges, so simulation inserts 1000 parts and waits 30 days to increase the age.', type: 'simple', color: 'success' },
    'simpleMergesWithInserts': { description: 'Periodic scenario with 1000 iterations of 10 inserts and simple merges', type: 'simple', color: 'success' },

    // Sequential Max Entropy Selector scenarios
    'maxEntropyMergesDemo': { description: 'Inserts 1000 parts and applies max-entropy merge strategy with adaptive scoring', type: 'entropy', color: 'warning' },
    'maxEntropyMergesWithInserts': { description: 'Periodic scenario with 1000 iterations of 10 inserts and max-entropy merges', type: 'entropy', color: 'warning' },

    // Layer based merging
    'periodicInsertsWith2BaseMerges': { description: 'It merge 2 parts on every layer', type: 'function', color: 'primary' },
    'periodicInsertsWith3BaseMerges': { description: 'It merge 3 parts on every layer', type: 'function', color: 'primary' },
    'periodicInsertsWith4BaseMerges': { description: 'It merge 4 parts on every layer', type: 'function', color: 'primary' },
    'periodicInsertsWith5BaseMerges': { description: 'It merge 5 parts on every layer', type: 'function', color: 'primary' },
    'periodicInsertsWith10BaseMerges': { description: 'It merge 10 parts on every layer', type: 'function', color: 'primary' },
    'periodicInsertsWithEBaseMerges': { description: 'It merge e (2.718) parts on every layer', type: 'function', color: 'primary' },
    'periodicInsertsWith10BaseMerges': { description: 'It merge 10 parts on every layer', type: 'function', color: 'primary' },
    'periodicInsertsWith10AndEBaseMerges': { description: 'It merge 10 parts on lowest layer and e (2.718) parts on every other layer', type: 'function', color: 'primary' }
};

// Tools that open separate HTML pages
const TOOLS = {
    'layers_model.html': { description: 'Modeling tool for fixed-base merge selector. It simulates merges of equal-sized initial parts without inserts. All parts are merged either into one final part or into a few max-sized parts. Tool enables optimization of the Integral through gradient descent or pre-trained bilinear interpolation (BLI). Optimization is done independently for different values of WA with a decision variabels represented by a vector of "Bases" that fixed-base merge selector takes as input. Note that any bases may be entered for simulation w/o optimization.', type: 'tool' },
    'solution_chart.html': { description: 'Interactive chart tool for comparing different solutions of the Layers Model. It visualizes a set of solutions as a series with selectable parameters, axis, solver or an optimization method.', type: 'tool' },
    'train_solver.html': { description: 'Trains layer-model BLI solvers using subgradient methods.', type: 'tool' },
    'float_layer_merges.html': { description: '[In-development] Modeling tool that is intended for simulating merges with inserts.', type: 'tool' }
};

function createNavigationPage() {
    // Hide all existing containers
    const containers = ['metrics-container', 'util-container', 'rewind-container', 'time-container', 'var-container'];
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
                <h2 class="text-info pb-2 mb-4">Tutorial</h2>
                <p>This section explains main concepts and visualizations (utility and time diagrams) on simple examples.
                Click ℹ signs for more information. Also note the summary of the simulation on top of the page:
                    <li>WA (write-amplification factor) - the ratio of the total bytes written to the size of the inserted data</li>
                    <li>Time - the total simulation time.</li>
                    <li>Integral - the number of active parts integrated over time. An important metric for an optimization.</li>
                    <li>AvgPartCount - the Integral divided by the Time. Less important for optimization but reflects average active part number throughout the simulation.</li>
                </p>
                <p>Compare above metrics for three different ways to merge 16 parts into one big final part.
                    Click on the buttons below to run different scenarios:</p>
                <div class="row" id="tutorial-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-success pb-2 mb-4">Simple Merge Selector (Sequential)</h2>
                <p>Simple merges demo. Scenarios show how default ClickHouse merge selector behaves. It is sequential (only one merge is selected and executed at a time) and non realistic demo, but it helps to understand the behaviour of simple merge selector.</p>
                <div class="row" id="simple-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-warning pb-2 mb-4">Max Entropy Selector (Sequential)</h2>
                <p>Simple selector uses heuristic to score the merge (based on max parts size to total size ratio), which sometimes leads to suboptimal choices.
                Max-entropy merge selector uses entropy as a score instead and always selects the merge with the highest possible entropy.
                It enables one to limit the total write-amplification factor by limiting lowest possible entropy of merges that selector is allowed to consider,
                but this requires knowledge of total data size. This demo is also sequential and only one merge is selected and executed at any time.
                Note that this selector and simple merge selector struggle with a fragmentation issue:
                when there are a lot of merge to select from it does selection optimally,
                but because it never looks forward it creates gaps with too few parts that has no way to be merged efficiently.</p>
                <div class="row" id="entropy-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-primary pb-2 mb-4">Parallel merging with fixed base and periodic inserts</h2>
                <p>Simulation with strictly periodic part inserts and parallel merging with fixed base.
                It uses special kind of merge selector that organize parts in layers depending on size.
                Every layer has associated base (number of parts to merge).
                In all simulations here parallel merges are allowed and the number of workers is unlimited.
                Simulation stops immediately after 10000 parts are inserted (no wait for all parts to be merged).
                Insertion rate is 20 parts per second of 10MB size. One worker merge speed is 100MB/s.  </p>
                <div class="row" id="functions-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-secondary pb-2 mb-4">Analysis Tools</h2>
                <div class="row" id="tools-grid"></div>
            </div>
        </div>
    `;

    // Populate scenarios
    const tutorialGrid = document.getElementById('tutorial-grid');
    const simpleGrid = document.getElementById('simple-grid');
    const entropyGrid = document.getElementById('entropy-grid');
    const functionsGrid = document.getElementById('functions-grid');
    const toolsGrid = document.getElementById('tools-grid');

    // Grid mapping configuration
    const gridMapping = {
        'tutorial': tutorialGrid,
        'simple': simpleGrid,
        'entropy': entropyGrid,
        'function': functionsGrid
    };

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

        const cardColor = page.color || 'secondary';
        const targetGrid = gridMapping[page.type] || functionsGrid;

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

        targetGrid.appendChild(card);
    });

    // Populate tools
    Object.entries(TOOLS).forEach(([filename, tool]) => {
        const card = document.createElement('div');
        card.className = 'col-lg-4 col-md-6 col-sm-12 mb-3';

        const toolName = filename.replace('.html', '');

        card.innerHTML = `
            <div class="p-3">
                <button class="btn btn-secondary btn-sm mb-2 w-100">${toTitleCase(toolName)}</button>
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
    const containers = ['metrics-container', 'util-container', 'rewind-container', 'time-container', 'var-container'];
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
    if (page.type === 'tutorial' || page.type === 'simple' || page.type === 'entropy') {
        demo(pageKey);
    } else if (pageKey === 'periodicInsertsWith2BaseMerges') {
        periodicInsertsWithBases([2, 2, 2, 2, 2, 2, 2, 2, 2, 2]);
    } else if (pageKey === 'periodicInsertsWith3BaseMerges') {
        periodicInsertsWithBases([3, 3, 3, 3, 3, 3, 3, 3, 3, 3]);
    } else if (pageKey === 'periodicInsertsWith4BaseMerges') {
        periodicInsertsWithBases([4, 4, 4, 4, 4, 4, 4, 4, 4, 4]);
    } else if (pageKey === 'periodicInsertsWith5BaseMerges') {
        periodicInsertsWithBases([5, 5, 5, 5, 5, 5, 5, 5, 5, 5]);
    } else if (pageKey === 'periodicInsertsWith10BaseMerges') {
        periodicInsertsWithBases([10, 10, 10, 10, 10, 10, 10, 10, 10, 10]);
    } else if (pageKey === 'periodicInsertsWithEBaseMerges') {
        periodicInsertsWithBases([Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E]);
    } else if (pageKey === 'periodicInsertsWith10AndEBaseMerges') {
        periodicInsertsWithBases([10, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E, Math.E]);
    }
}

async function demo(scenarioName)
{
    const simContainer = new SimulationContainer();
    const mt = runScenario(scenarioName);
    simContainer.update({mt}, true);
    if (mt.time > 30 * 86400)
        simContainer.rewinder.setMinTime(30 * 86400);
}

async function periodicInsertsWithBases(layerBases)
{
    const simContainer = new SimulationContainer();
    const insertPartSize = 10 << 20;
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
        pool_size: 10000, // We want unlimited pool size here
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
