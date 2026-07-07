import { assert } from 'https://cdn.jsdelivr.net/npm/chai/chai.js';

import { EventSimulator } from '../EventSimulator.js';
import { MergeTree } from '../MergeTree.js';
import { MergeTreeInserter } from '../MergeTreeInserter.js';

export async function testMergeTreeInserter()
{
    function* testInserter1(mt)
    {
        yield {type: 'insert', bytes: 1};
    }

    function* testInserter2(mt)
    {
        yield {type: 'insert', bytes: 42};
        yield {type: 'insert', bytes: 42};
        yield {type: 'insert', bytes: 42};
        yield {type: 'sleep', delay: 1.5};
        yield {type: 'insert', bytes: 13};
        yield {type: 'insert', bytes: 13};
        yield {type: 'sleep', delay: 10};
        yield {type: 'insert', bytes: 666};
    }

    function* testInserter3(mt)
    {
        for (let i = 1; i < 5; i++)
        {
            yield {type: 'sleep', delay: 1};
            yield {type: 'insert', bytes: i};
        }
    }

    const sim = new EventSimulator();
    const mt = new MergeTree();
    const inserter1 = new MergeTreeInserter(sim, mt, testInserter1());
    const inserter2 = new MergeTreeInserter(sim, mt, testInserter2());
    const inserter3 = new MergeTreeInserter(sim, mt, testInserter3());

    await inserter1.start();
    await inserter2.start();
    await inserter3.start();
    await sim.run();

    assert.deepEqual(mt.parts.map(d => d.bytes), [1, 42, 42, 42, 1, 13, 13, 2, 3, 4, 666]);

    console.log('Test 1 passed: MergeTreeInserter');
}
