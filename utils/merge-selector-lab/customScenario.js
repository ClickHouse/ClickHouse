import { MergeTree } from './MergeTree.js';
import { MergeTreeMerger } from './MergeTreeMerger.js';
import { MergeTreeInserter } from './MergeTreeInserter.js';
import { EventSimulator } from './EventSimulator.js';
import { WorkerPool } from './WorkerPool.js';

export async function customScenario(scenario, signals)
{
    const {inserts, selector, pool_size} = scenario;

    // Setup discrete event simulation
    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, pool_size);
    const mt = new MergeTree();
    const inserters = inserts.map(inserter => new MergeTreeInserter(sim, mt, inserter, signals));
    const merger = new MergeTreeMerger(sim, mt, pool, selector, signals);

    // Start agents
    for (const inserter of inserters)
        await inserter.start();
    await merger.start();

    // Make timer
    const {on_every_real_second, on_every_frame} = signals;
    if (on_every_real_second)
        sim.addTimer(1000, async () => await on_every_real_second({mt, sim}));
    const target_fps = 60.0;
    if (on_every_frame)
        sim.addTimer(1000.0 / target_fps, async () => await on_every_frame({mt, sim}));

    // Run the simulation
    await sim.run();

    // Return resulting merge tree
    return mt;
}
