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
        this.max_workers = undefined; // Limit on the number of worker to work on the merges

        // Average size of parts [bytes]
        this.source_size = undefined;
        this.target_size = undefined;

        this.workers = 0;         // Workers assign to do merges in this group currently
        this.parent = undefined;  // Group that contain target parts
        this.children = [];       // Groups that were merge into this group

        // Timestamps of group lifecycle
        this.sealed = undefined;    // Group sealed (target size set and children added)
        this.merged = undefined;    // Group merged from its children groups
        this.attached = undefined;  // Group started merging into its parent group

        // Metrics that are updated with this.time
        this.time = time;
        this.deadline = undefined; // Merge finish timestamp for merge job with workers
        this.bytes_written = 0;    // Number of bytes written during merge job
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
        this.sealed = time;
        this.log_bytes = Math.log2(this.bytes);

        // A better model will take into account only number of bytes left `this.bytes - this.bytes_written`
        // But it would mean that workers should be released one by one, which is not always correct
        // So to simplify we instead keep all workers busy until the end
        this.max_workers = this.bytes / this.target_size;
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
        if (this.bytes < this.target_size)
            throw { message: "Attempt to seal group with too high target size", group: this };
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

        // Parts inside groups merging into the parent group are accounted by the parent
        if (this.parent) {
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

    // Add workers into merge job
    addWorkers(time, workers) {
        this.#advance(time);
        this.workers += workers;
        if (this.workers > this.max_workers)
            throw { message: "Attempt to assign too many workers to a merge job", group: this };
        this.deadline = this.time + (this.bytes - this.bytes_written) / (WORKER_SPEED * this.workers);
    }

    // Group was fully merged from children groups (should be called at mergeTimestamp)
    // Returns the number of workers that have been released from the merge job
    merge(time) {
        this.#advance(time);
        this.merged = time;
        const result = this.workers;
        this.workers = 0;
        this.deadline = undefined;
        return result;
    }

    // Group begins merging into parent group
    attach(time, parent) {
        this.#advance(time); // Account for waiting time w/o merges
        if (this.parent)
            throw { message: "Attempt to attach group that already has parent", group: this};
        this.attached = time;
        this.parent = parent;
    }
}

// Plan for merging that collect parts of the similar size into groups, reducing dimention of the scheduling problem
// Groups are then organized into a tree, which allows to compute overall cost of merging in terms of bytes and part-time-integral
export class MergingPlan {
    constructor(total_workers) {
        // Totals
        this.total_parts = 0;
        this.total_bytes = 0;
        this.total_utility = 0;
        this.total_workers = total_workers;
        this.total_bytes_written = 0;
        this.total_integral_active_part_count = 0;

        // Aggregates
        this.utility = 0; // utility sum over all merges in the plan
        this.entropy = 0; // utility per byte

        // Current state of planning
        this.time = 0;
        this.available_workers = total_workers;

        // Merging groups
        this.merging_groups = [];

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

    // Create a new merge job and corresponding group
    startMerge(groups_to_merge, target_size, workers) {
        const result = new PartGroup();
        for (let g of groups_to_merge) {
            g.attach(this.time, result);
            result.addGroup(g);
        }
        result.seal(this.time, target_size);
        this.available_workers -= workers;
        result.addWorkers(workers);
        this.merging_groups.push(result);
        return result;
    }

    // Finishes the merge job and redistribute workers
    finishMerge(group) {
        const workers_released = group.merge(this.time);
        this.available_workers += workers_released;
        this.insertGroup(group);

        // Redistribute workers to existing merge jobs if possible
        for (const g of this.merging_groups) {
            const demand = g.max_workers - g.workers;
            if (demand > 0) {
                const allocated = Math.min(this.available_workers, demand);
                g.addWorkers(allocated);
                this.available_workers -= allocated;
                if (this.available_workers <= 0)
                    break;
            }
        }
    }

    // Build the plan according to given `strategy` and computes total costs
    // Should be called after initialize()
    build(strategy) {
        while (true) {
            // Run strategy until we run out of work or workers
            if (this.available_workers > 0 && this.current_groups.size() > 0) {
                strategy(this);
                continue;
            }
            // Then advance time to the next group merged
            if (this.merging_groups.length > 0) {
                // Find and pop group that finish the merge first
                const group_index = this.merging_groups.reduce((minIdx, cur, idx, arr) => cur.deadline < arr[minIdx].deadline ? idx : minIdx, 0);
                const [merged_group] = this.merging_groups.splice(group_index, 1);
                this.time = merged_group.deadline;
                this.finishMerge(merged_group);
                continue;
            }
            break; // All groups processed
        }
    }

    // Add group as available for scheduling a new merge
    insertGroup(group) {
        // Update metrics
        this.total_bytes_written = 0;
        this.total_integral_active_part_count = 0;
        this.groups.push(group);

        // Make group available for futher merge scheduling
        if (group.target_size == this.total_bytes)
            return; // This is the final group having all bytes merged. We are done.
        this.current_groups.push(group);
    }
};
