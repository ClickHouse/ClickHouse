// MergeTreeInserter is used with EventSimulator to INSERT new part according to given `inserter` schedule
export class MergeTreeInserter
{
    constructor(sim, mt, inserter, signals = {})
    {
        this.sim = sim; // EventSimulator
        this.mt = mt; // MergeTree
        this.inserter = inserter;
        this.signals = signals;
    }

    async start()
    {
        await this.#iterateInserter();
    }

    async #iterateInserter()
    {
        while (true)
        {
            const { value, done } = await this.inserter.next();
            if (done)
            {
                if (this.signals.on_inserter_end)
                    await this.signals.on_inserter_end({sim: this.sim, mt: this.mt, inserter: this.inserter});
                return; // No more inserts
            }
            switch (value.type)
            {
                case 'insert':
                    this.mt.advanceTime(this.sim.time);
                    const part = this.mt.insertPart(value.bytes);
                    if (this.signals.on_insert)
                        await this.signals.on_insert({sim: this.sim, mt: this.mt, inserter: this.inserter, part});
                    break;
                case 'sleep':
                    if (value.delay > 0)
                    {
                        this.sim.scheduleAt(this.sim.time + value.delay, "InserterSleep", async () => await this.#iterateInserter());
                        return;
                    }
                    break;
                default:
                    throw { message: "Unknown merge tree inserter yield type", value};
            }
        }
    }
}
