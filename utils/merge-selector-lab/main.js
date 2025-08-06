import { SimulationContainer } from './SimulationContainer.js';
import { runScenario } from './Scenarios.js';
import { sequenceInserter } from './sequenceInserter.js';
import { customScenario } from './customScenario.js';
import { floatLayerMerges } from './floatLayerMerges.js';
import { delayMs } from './util.js';

// Global pages configuration including scenarios and other functions
export const PAGES = {
    // Tutorial scenarios
    'explainVisualizations': { description: 'Small demo with 20 parts using max-entropy merge strategy with 7 merges', type: 'tutorial', color: 'info' },
    'binaryTree': { description: 'Creates 16 equal-sized parts and merges them in a binary tree pattern', type: 'tutorial', color: 'info' },
    'aggressiveMerging': { description: 'Demonstrates aggressive merging by merging all parts after each insert', type: 'tutorial', color: 'info' },
    'oneBigMerge': { description: 'Creates 16 parts and merges them all into one large part', type: 'tutorial', color: 'info' },
    'randomMess': { description: 'Creates chaotic state with 100 random inserts followed by 30 random merges', type: 'tutorial', color: 'info' },

    // Sequential Simple Selector scenarios
    'simpleMergesDemo': { description: 'Inserts 1000 parts and applies simple merge strategy', type: 'simple', color: 'success' },
    'simpleMergesWithInserts': { description: 'Periodic scenario with 1000 iterations of 10 inserts and simple merges', type: 'simple', color: 'success' },

    // Sequential Max Entropy Selector scenarios
    'maxEntropyMergesDemo': { description: 'Long-running demo with 300 iterations of inserts and max-entropy merges', type: 'entropy', color: 'warning' },
    'maxEntropyDemo': { description: 'Inserts 1000 parts and applies max-entropy merge strategy with adaptive scoring', type: 'entropy', color: 'warning' },
    'maxEntropyWithInserts': { description: 'Periodic scenario with 1000 iterations of 10 inserts and max-entropy merges', type: 'entropy', color: 'warning' },

    // Add other functions (parallel merging)
    'periodicArrivals': { description: 'Simulation with periodic part arrivals and float layer merges (parallel merging)', type: 'function', color: 'primary' }
};

// Tools that open separate HTML pages
export const TOOLS = {
    'solution_chart.html': { description: 'Interactive chart tool for comparing merge optimization solutions across different parameters', type: 'tool' },
    'layers_model.html': { description: 'Modeling tool for analyzing layered merge strategies with configurable bases and parameters', type: 'tool' },
    'train_solver.html': { description: 'Machine learning tool for training merge optimization solvers using gradient descent methods', type: 'tool' },
    'float_layer_merges.html': { description: 'Specialized simulation for float layer merge strategies with sequence insertion patterns', type: 'tool' }
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
                <p class="text-muted">Explain visualizations, binary tree, aggressive merging, one big merge, random mess</p>
                <div class="row" id="tutorial-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-success pb-2 mb-4">Sequential Simple Selector</h2>
                <p class="text-muted">Simple merges demo, simple merges with inserts</p>
                <div class="row" id="simple-grid"></div>
            </div>
        </div>
        <div class="row mt-5">
            <div class="col-12">
                <h2 class="text-warning pb-2 mb-4">Sequential Max Entropy Selector</h2>
                <p class="text-muted">Max entropy merges demo, max entropy demo, max entropy with inserts</p>
                <div class="row" id="entropy-grid"></div>
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
