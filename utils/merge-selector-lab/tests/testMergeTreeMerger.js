import { assert } from 'https://cdn.jsdelivr.net/npm/chai/chai.js';

import { EventSimulator } from '../EventSimulator.js';
import { WorkerPool } from '../WorkerPool.js';
import { MergeTree } from '../MergeTree.js';
import { MergeTreeMerger } from '../MergeTreeMerger.js';
import { MergeTreeUtilityVisualizer, MergeTreeTimeVisualizer } from '../MergeTreeVisualizer.js';

export async function testMergeTreeMerger()
{
    // Merge selector for test. It merges every two adjacent active parts.
    function* testSelector()
    {
        const mt = yield {type: 'getMergeTree'};
        while (true)
        {
            if (mt.active_part_count < 2)
                return; // no more merges possible
            const active_parts = mt.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
            let merge_started = false;
            for (let i = 0; i < active_parts.length - 1; i++)
            {
                const left = active_parts[i];
                const right = active_parts[i + 1];
                if (!left.merging && !right.merging)
                {
                    yield {type: 'merge', parts_to_merge: [left, right]};
                    merge_started = true;
                    break;
                }
            }
            if (!merge_started)
                yield {type: 'wait'}; // we have to wait for any merge to finish before proceeding
        }
    }

    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, 4); // 4 workers available for parallel execution of level-1 merges
    const mt = new MergeTree();

    let expected_bytes = 0;
    for (let i = 1; i <= 8; i++)
    {
        const bytes = i * (100 << 20);
        mt.insertPart(bytes);
        expected_bytes += bytes;
    }

    const merger = new MergeTreeMerger(sim, mt, pool, testSelector());
    await merger.start();
    await sim.run();

    assert.deepEqual(mt.parts.filter(d => d.active).map(d => d.bytes), [expected_bytes]);

    console.log('Test 1 passed: MergeTreeMerger');

    // Show result
    d3.select("body")
        .append("div")
        .attr("class", "container-fluid")
        .append("div")
        .attr("class", "row")
        .call(function(row) {
            row.append("div")
                .attr("class", "col-md-6")
                .attr("id", "merge-tree-merge-time-container");
            row.append("div")
                .attr("class", "col-md-6")
                .attr("id", "merge-tree-merge-util-container");
        });

    new MergeTreeUtilityVisualizer(mt, d3.select("#merge-tree-merge-util-container"));
    new MergeTreeTimeVisualizer(mt, d3.select("#merge-tree-merge-time-container"));
}
