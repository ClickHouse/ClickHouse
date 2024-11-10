import { assert } from 'https://cdn.jsdelivr.net/npm/chai/chai.js';

import { EventSimulator } from '../EventSimulator.js';
import { WorkerPool } from '../WorkerPool.js';
import { MergeTree } from '../MergeTree.js';
import { MergeTreeMerger } from '../MergeTreeMerger.js';
import { visualizeExecutionTime } from '../visualizeExecutionTime.js';
import { visualizeUtility } from '../visualizeUtility.js';

export async function testMergeTreeMerger() {
    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, 4); // 4 workers available for parallel execution of level-1 merges
    const mt = new MergeTree();
    const merger = new MergeTreeMerger(sim, mt, pool);

    let level0 = [];
    for (let i = 1; i <= 8; i++) {
        level0.push(merger.insert(i * (100 << 20)));
    }

    const level1 = [
        merger.merge([level0[0], level0[1]]),
        merger.merge([level0[2], level0[3]]),
        merger.merge([level0[4], level0[5]]),
        merger.merge([level0[6], level0[7]]),
    ];

    const level2 = [
        merger.promiseMerge([level1[0], level1[1]]),
        merger.promiseMerge([level1[2], level1[3]]),
    ];

    const level3 = [
        merger.promiseMerge([level2[0], level2[1]]),
    ];

    await sim.run();

    // Show result
    d3.select("body")
        .append("div")
        .attr("class", "container-fluid")
        .append("div")
        .attr("class", "row")
        .call(function(row) {
            row.append("div")
                .attr("class", "col-md-6")
                .attr("id", "merge-tree-merge-exec-container");
            row.append("div")
                .attr("class", "col-md-6")
                .attr("id", "merge-tree-merge-util-container");
        });

    visualizeUtility(mt, d3.select("#merge-tree-merge-util-container"), true);
    visualizeExecutionTime(mt, d3.select("#merge-tree-merge-exec-container"));
}
