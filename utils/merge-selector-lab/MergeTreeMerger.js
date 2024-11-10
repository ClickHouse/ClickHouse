import { promiseCombine } from './promiseCombine.js';

export class MergeTreeMerger
{
    constructor(sim, mt, pool)
    {
        this.sim = sim; // EventSimulator
        this.mt = mt; // MergeTree
        this.pool = pool; // WorkerPool
    }

    insert(bytes)
    {
        return this.mt.insertPart(bytes, this.sim.time);
    }

    merge(parts_to_merge)
    {
        this.mt.beginMergeParts(parts_to_merge);

        // Calculate the duration based on number of bytes to be written
        const bytes = parts_to_merge.reduce((sum, d) => sum + d.bytes, 0);
        const mergeDuration = this.mt.mergeDuration(bytes, parts_to_merge.length);

        // Create a Promise that will be resolved on merge finish
        const future = new Promise((resolve, reject) => {
            // Schedule the merge using the worker pool
            this.pool.schedule(mergeDuration, () => {
                try {
                    this.mt.advanceTime(this.sim.time);
                    const resulting_part = this.mt.finishMergeParts(parts_to_merge);
                    resolve(resulting_part);
                } catch (error) {
                    reject(error);
                }
            });
        });

        // Return the promise object
        return future;
    }

    // Like merge, but takes array of promises for parts
    promiseMerge(future_parts_to_merge)
    {
        return promiseCombine(
            future_parts_to_merge,
            (parts_to_merge) => this.merge(parts_to_merge),
            () => this.sim.ping());
    }
}
