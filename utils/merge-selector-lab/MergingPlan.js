import { MergeTree } from './MergeTree.js';
import Heap from 'https://cdn.skypack.dev/heap-js';

const GROUPS_PER_SIZE_DOUBLING = 4;

// Description of a group of parts of similar size, that only hold aggregate value required for planning
export class PartGroup {
    constructor() {
        this.parts = 0;
        this.bytes = 0;
        this.utility = 0;
    }

    addPart(part) {
        this.parts++;
        this.bytes += part.bytes;
        this.utility += part.utility;
    }

    // Should be called when all parts are added
    created(time) {
        this.created = time;
        this.log_bytes = Math.log2(this.bytes);
    }
}

// Plan for merging that collect parts of the similar sizes into groups, reducing dimention of the scheduling problem
export class MergingPlan {
    constructor(total_workers) {
        // Totals
        this.total_parts = 0;
        this.total_bytes = 0;
        this.total_utility = 0;
        this.total_workers = total_workers;

        // Aggregates
        this.utility = 0; // utility sum over all merges in the plan
        this.entropy = 0; // utility per byte

        // Current state of planning
        this.time = 0;
        this.available_workers = total_workers;
        this.events = new Heap((a, b) => a.time - b.time); // Use min-heap for pending events
        this.current_groups = []; // Only include groups that are available for merging at this.time, sorted by bytes

        // Array of all part groups
        this.groups = [];
    }

    // Group active parts of a merge tree
    initialize(mt) {
        const initial_groups = {};
        for (const part of mt.parts) {
            if (!part.active) {
                continue;
            }
            // Note that we also take merging parts into account to make a robust plan
            const key = Math.floor(p.log_bytes * GROUPS_PER_SIZE_DOUBLING);
            if (!(key in initial_groups))
                initial_groups[key] = new PartGroup();
            initial_groups[key].addPart(part);
            this.total_parts++;
            this.total_bytes += part.bytes;
            this.total_utility += part.utility;
        }

        for (const key in initial_groups) {
            createGroup(initial_groups[key]);
        }

        this.utility = this.total_bytes * Math.log2(this.total_bytes) - this.total_utility;
        this.entropy = this.utility / this.total_bytes;
    }

    createGroup(group) {
        group.created(this.time);
        this.groups.push(group);
    }
};
