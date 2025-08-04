import { MergeTree } from './MergeTree.js';
import Heap from 'https://cdn.skypack.dev/heap-js';

// Number of groups to create initially between x and 2*x byte sizes.
// Initial group boundaries will be: 2**(i/GROUPS_PER_SIZE_DOUBLING)
const GROUPS_PER_SIZE_DOUBLING = 4;

// Number of bytes worker handles per second. [bytes/second]
// This is a coefficent that should not affect the final result, but we keep it to track units in formulas.
const WORKER_SPEED = 1;

// Description of a group of parts of similar size, that only hold aggregate value required for planning
export class PartGroup {
    constructor() {
        this.parts = 0;               // Number of parts in group
        this.bytes = 0;               // Total size of parts in group
        this.log_bytes = undefined;   // log2(bytes)
        this.utility = 0;             // Sum of utility of source parts

        // Average size of parts [bytes]
        this.source_size = undefined;
        this.target_size = undefined;

        this.active = true;       // Does group contain at least one active part currently
        this.merging = false;     // Does group participate in merge job currently
        this.workers = 0;         // Workers assign to do merges in this group currently
        this.parent = undefined;  // Group that contain target parts
        this.children = [];       // Groups that were merge into this group

        this.sealed = undefined;    // Group sealed (target size and children se)
        this.merged = undefined;
        this.started = undefined;
        this.finished = undefined;

        // Metrics that are updated with this.time
        this.time = time;
        this.deadline = undefined; // Merge finish timestamp for merge job with workers
        this.bytes_written = 0; // Number of bytes written by
        this.integral_active_part_count = 0;
    }

    // Add a part (only for initial groups)
    addPart(part) {
        this.parts++;
        this.bytes += part.bytes;
        this.utility += part.utility;
    }

    // Add a child group
    addGroup(group) {
        this.parts += group.parts;
        this.bytes += group.bytes;
        this.utility += group.utility;
        this.children.push(group);
    }

    #onSeal(time) {
        this.log_bytes = Math.log2(this.bytes);
        this.sealed = time;
    }

    // Group is fully formed.
    // Should be called when all groups are added
    seal(time, target_size) {
        if (this.parts == 0)
            throw { message: "Attempt to seal an empty group", group: this };
        this.source_size = this.bytes / this.parts;
        this.target_size = target_size;
        if (this.target_size < this.source_size)
            throw { message: "Attempt to seal group with too low target size", group: this };
        this.parts = this.bytes / target_size;
        this.#onSeal(time);
    }

    // Group is fully formed.
    // Should be called when all parts are added into initial group
    sealInitial(time) {
        if (this.parts == 0)
            throw { message: "Attempt to seal an empty group", group: this };
        this.source_size = this.bytes / this.parts;
        this.target_size = this.source_size;
        this.#onSeal(time);
    }

    // Update metrics due to time advance
    #advance(time) {
        if (this.time > time)
            throw { message: "Attempt to advance time backward for group", group: this };
        if (this.time == time)
            return;

        // Parts inside merging groups are accounted by the parent
        if (this.merging || !this.active) {
            this.time = time;
            return;
        }

        const time_delta = time - this.time;
        const bytes = time_delta * WORKER_SPEED * this.workers;

        // Compute number of bytes before and after advance
        const bytes_before = this.bytes_written;
        const bytes_after = Math.min(this.bytes, bytes_before + bytes);

        // Compute number of parts in groups before and after advance based on bytes written
        const parts_before = bytes_before / this.target_size + (this.bytes - bytes_before) / this.source_size;
        const parts_after  = bytes_after  / this.target_size + (this.bytes - bytes_after ) / this.source_size;

        // Assume the number of parts is changing in a linear way with time
        // TODO(serxa): should we update it in a way that is consistent with model solver?
        // TODO(serxa): or should we update solver model to be consistent? or do nothing?
        this.integral_active_part_count += time_delta * (parts_after + parts_before) / 2;
        this.bytes_written = bytes_after;
        this.time = time;
    }

    // Returns time at which merge job should be finished given current number of workers and previous progress
    #mergeTimestamp() {
        return this.time + (this.bytes - this.bytes_written) / (WORKER_SPEED * this.workers);
    }

    // Add workers into merge job
    addWorkers(time, workers) {
        this.#advance(time);
        this.workers += workers;
        this.deadline = this.#mergeTimestamp();
    }

    // Group was fully merged from children groups (should be called at mergeTimestamp)
    // Returns number of workers that was released from the merge job
    merge(time) {
        this.#advance(time);
        this.merged = time;
        const result = this.workers;
        this.workers = 0;
        this.deadline = undefined;
        return result;
    }

    // Group begins merging into parent group
    start(time, parent) {
        this.#advance(time);
        if (this.active == false)
            throw { message: "Attempt to start merge of inactive group", group: this };
        if (this.merging == true)
            throw { message: "Attempt to start merge of group that already participates in another merge", group: this};
        this.started = time;
        this.merging = true;
        this.parent = parent;
    }

    // Group was merged into parent group
    finish(time) {
        this.#advance(time);
        if (this.active == false)
            throw { message: "Attempt to finish merge of inactive group", group: this };
        if (this.merging == false)
            throw { message: "Attempt to finish merge without prior start merge", group: this };
        this.finished = time;
        this.merging = false;
        this.active = false;
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

        // Merging groups min-heap, ordered by merge finish timestamp
        this.merging_groups = new Heap((a, b) => a.deadline - b.deadline);

        // Groups available for merging min-heap, ordered by average part size
        this.current_groups = new Heap((a, b) => a.target_size - b.target_size);

        // Array of all part groups (append only)
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
            initial_groups[key].sealInitial(this.time);
            insertGroup(initial_groups[key]);
        }

        this.utility = this.total_bytes * Math.log2(this.total_bytes) - this.total_utility;
        this.entropy = this.utility / this.total_bytes;
    }

    startMerge(groups_to_merge, target_size, workers) {
        const result = new PartGroup();
        for (let g of groups_to_merge) {
            g.start(this.time, result);
            result.addGroup(g);
        }
        result.seal(this.time, target_size);
        result.addWorkers(workers);
        this.merging_groups.push(result);
        return result;
    }

        // group.addWorkers(workers);
    }

    finishMerge(group) {
        for (let g of group.children) {
            g.finish();
        }
        const workers_released = group.merge(this.time);
        this.insertGroup(group);
        this.available_workers += workers_released;

        // TODO(serxa): we should rebuild heap,
        // becuase workers could only be added to groups that are not saturated,
        // and we should traverse in target order size!
        // if (this.merging_groups.size() > 0) {
        //     this.merging_groups.peek().addWorkers()
        // }
    }

    // Add group as available for scheduling a new merge
    insertGroup(group) {
        this.groups.push(group);
        this.current_groups.push(group);
    }
};
