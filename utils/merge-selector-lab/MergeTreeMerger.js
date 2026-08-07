// MergeTreeMerger is used with EventSimulator to run MergeTree merges in parallel using WorkerPool
export class MergeTreeMerger
{
    constructor(sim, mt, pool, selector, signals = {})
    {
        this.sim = sim; // EventSimulator
        this.mt = mt; // MergeTree
        this.pool = pool; // WorkerPool
        this.selector = selector;
        this.signals = signals;
        this.merges_running = 0;
        // TODO(serxa): if we are going to use more than 1 selector in parallel we should "subscribe" for pool availability to be able to run more merges when worker is release by another merger
    }

    async start()
    {
        this.mt.addInsertObserver(() => this.sim.postpone("MergerInsertObserver", async () => await this.#iterateSelector()));
        if (this.mt.active_part_count == 0) // Hack to start only after initial parts are inserted
            this.sim.postpone("PostponeMergerInit", async () => await this.#iterateSelector());
        else
            await this.#iterateSelector();
    }

    async #iterateSelector()
    {
        let value_to_send = null;
        while (this.pool.isAvailable())
        {
            const { value, done } = await this.selector.next(value_to_send);
            value_to_send = null;
            if (done)
                return; // No more merges required
            switch (value.type)
            {
                case 'getMergeTree':
                    value_to_send = this.mt;
                    break;
                case 'merge':
                    const {parts_to_merge} = value;
                    this.#beginMerge(parts_to_merge);
                    if (this.signals.on_merge_begin)
                        await this.signals.on_merge_begin({sim: this.sim, mt: this.mt, parts_to_merge});
                    break;
                case 'wait':
                    return; // No need to do anything iterateSelector() will be called on insert or the end of any merge
                default:
                    throw { message: "Unknown merge selector yield type", value };
            }
        }
    }

    #beginMerge(parts_to_merge)
    {
        this.mt.advanceTime(this.sim.time);
        this.mt.beginMergeParts(parts_to_merge);
        this.merges_running++;

        // Calculate the duration based on number of bytes to be written
        const bytes = parts_to_merge.reduce((sum, d) => sum + d.bytes, 0);
        const mergeDuration = this.mt.mergeDuration(bytes, parts_to_merge.length);

        // Schedule merge finish event
        return this.pool.schedule(mergeDuration, "MergeEnd", async (sim, event) => await this.#onMergeEnd(parts_to_merge));
    }

    async #onMergeEnd(parts_to_merge)
    {
        this.mt.advanceTime(this.sim.time);
        let part = this.mt.finishMergeParts(parts_to_merge);
        if (this.signals.on_merge_end)
            await this.signals.on_merge_end({sim: this.sim, mt: this.mt, part, parts_to_merge});
        this.merges_running--;
        await this.#iterateSelector();
    }

    async #onInsert(part)
    {
        await this.#iterateSelector();
    }
}
