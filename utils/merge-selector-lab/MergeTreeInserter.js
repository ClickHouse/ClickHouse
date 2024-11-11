// MergeTreeInserter is used with EventSimulator to INSERT new part according to given `inserter` schedule
export class MergeTreeInserter
{
    constructor(sim, mt, inserter)
    {
        this.sim = sim; // EventSimulator
        this.mt = mt; // MergeTree
        this.inserter = inserter; // schedule of INSERTs
    }

    // Call start once to initiate insertion process
    start(opts = {})
    {
        this.inserterInstance = this.inserter(this.mt, opts);
        this.#iterateInserter();
    }

    #iterateInserter()
    {
        while (true)
        {
            const { value, done } = this.inserterInstance.next();
            if (done)
                return; // No more merges required
            switch (value.type)
            {
                case 'insert':
                    this.mt.insertPart(value.bytes);
                    break;
                case 'sleep':
                    this.sim.scheduleAt(this.sim.time + value.delay, () => this.#iterateInserter());
                    return;
                default:
                    throw { message: "Unknown merge inserter yield type", value};
            }
        }
    }
}
