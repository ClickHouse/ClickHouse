import { MergeTree } from './MergeTree.js';
import { MergeTreeMerger } from './MergeTreeMerger.js';
import { MergeTreeInserter } from './MergeTreeInserter.js';
import { EventSimulator } from './EventSimulator.js';
import { WorkerPool } from './WorkerPool.js';

export async function customScenario(scenario)
{
    const {inserts, selector, pool_size} = scenario;

    // Setup discrete event simulation
    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, pool_size);
    const mt = new MergeTree();
    const inserters = inserts.filter(inserter => new MergeTreeInserter(sim, mt, inserter));
    const merger = new MergeTreeMerger(sim, mt, pool, selector);

    // Run the simulation
    await sim.run();

    // Return resulting merge tree
    return mt;
}
