// MergeTreeMerger is used with EventSimulator to run MergeTree merges in parallel using WorkerPool
export class MergeTreeMerger
{
    constructor(sim, mt, pool, selector)
    {
        this.sim = sim; // EventSimulator
        this.mt = mt; // MergeTree
        this.pool = pool; // WorkerPool
        this.selector = selector; // One of selection algorithms (e.g. simpleMerges)
        this.merges_running = 0;
        this.retry_timeout = 1; // If zero merges are running, retry in 1 second
    }

    // Call start once to initiate merging process during simulation or to restart with new opts
    start(opts = {})
    {
        this.selectorInstance = this.selector(this.mt, opts);
        this.#iterateSelector();
        // TODO(serxa): if we are going to use more than 1 selector in parallel we should "subscribe" for pool availability to be able to run more merges when worker is release by another merger
    }

    #iterateSelector()
    {
        loop: while (this.pool.isAvailable())
        {
            const { value, done } = this.selectorInstance.next();
            if (done)
                return; // No more merges required
            switch (value.type)
            {
                case 'merge':
                    this.#beginMerge(value.parts_to_merge);
                    break;
                case 'wait':
                    break loop; // No need to do anything iterateSelector() will be called on the end of any merge
                default:
                    throw { message: "Unknown merge selector yield type", type, value};
            }
        }
        if (this.merges_running == 0)
            this.sim.scheduleAt(this.sim.time + this.retry_timeout, () => this.#iterateSelector());
    }

    #beginMerge(parts_to_merge)
    {
        this.mt.beginMergeParts(parts_to_merge);
        this.merges_running++;

        // Calculate the duration based on number of bytes to be written
        const bytes = parts_to_merge.reduce((sum, d) => sum + d.bytes, 0);
        const mergeDuration = this.mt.mergeDuration(bytes, parts_to_merge.length);

        // Create a Promise that will be resolved on merge finish
        return this.pool.schedule(mergeDuration, (sim, event) => this.#onMergeEnd(parts_to_merge));
    }

    #onMergeEnd(parts_to_merge)
    {
        this.mt.advanceTime(this.sim.time);
        this.mt.finishMergeParts(parts_to_merge);
        this.merges_running--;
        this.#iterateSelector();
    }
}
