// MergeTreeMerger is used with EventSimulator to run MergeTree merges in parallel using WorkerPool
export class MergeTreeMerger
{
    constructor(sim, mt, pool, selector)
    {
        this.sim = sim; // EventSimulator
        this.mt = mt; // MergeTree
        this.pool = pool; // WorkerPool
        this.selector = selector;
        this.merges_running = 0;
        if (mt.active_part_count == 0) // Hack to start only after initial parts are inserted
            this.sim.postpone("PostponeMergerInit", () => this.#iterateSelector());
        else
            this.#iterateSelector();
        // TODO(serxa): if we are going to use more than 1 selector in parallel we should "subscribe" for pool availability to be able to run more merges when worker is release by another merger
    }

    #iterateSelector()
    {
        let value_to_send = null;
        loop: while (this.pool.isAvailable())
        {
            const { value, done } = this.selector.next(value_to_send);
            value_to_send = null;
            if (done)
                return; // No more merges required
            switch (value.type)
            {
                case 'getMergeTree':
                    value_to_send = this.mt;
                    break;
                case 'merge':
                    this.#beginMerge(value.parts_to_merge);
                    break;
                case 'wait':
                    if (this.merges_running == 0)
                        throw { message: "Merge selector wait for zero merges. Run at least one or use 'sleep' instead" };
                    break loop; // No need to do anything iterateSelector() will be called on the end of any merge
                case 'sleep':
                    this.sim.scheduleAt(this.sim.time + value.delay, "MergerSleep", () => this.#iterateSelector());
                    return;
                default:
                    throw { message: "Unknown merge selector yield type", value };
            }
        }
        if (this.merges_running == 0)
            throw { message: "Pool is unavailable although 0 merges are running" };
    }

    #beginMerge(parts_to_merge)
    {
        this.mt.beginMergeParts(parts_to_merge);
        this.merges_running++;

        // Calculate the duration based on number of bytes to be written
        const bytes = parts_to_merge.reduce((sum, d) => sum + d.bytes, 0);
        const mergeDuration = this.mt.mergeDuration(bytes, parts_to_merge.length);

        // Create a Promise that will be resolved on merge finish
        return this.pool.schedule(mergeDuration, "MergeEnd", (sim, event) => this.#onMergeEnd(parts_to_merge));
    }

    #onMergeEnd(parts_to_merge)
    {
        this.mt.advanceTime(this.sim.time);
        this.mt.finishMergeParts(parts_to_merge);
        this.merges_running--;
        this.#iterateSelector();
    }
}
