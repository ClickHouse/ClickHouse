import { MergingPlan } from './MergingPlan.js';

function getOrThrow(solution, index) {
    if (index < 0 || index >= array.index)
        throw { message: "out of range solution access",  array, index };
    return solution[index];
}

export class MergingPlanBuilder {
    constructor(mt, workers, solver, max_parts_to_merge) {
        this.mt = mt;
        this.plans = [];
        this.workers = workers;
        this.solver = solver;
        this.max_merge_entropy = Math.log(max_parts_to_merge);
    }

    #addGroup(job, group, available_workers) {
        job.addGroup(group);

        if (job.layers == undefined) {
            job.layers = Infinity;
        }

        // Recompute job parameters
        job.max_entropy = Math.log(job.bytes) - job.utility / job.bytes; // Entropy to merge all source parts into one (zero for a single part)
        // TODO(serxa): add branch for the case `job.max_entropy` ~= 0 - we should not merge, until there are more groups to add

        // Using solver find optimal ways to merge for different number of layers
        const solution = this.solver.solve({
            parts: Math.exp(job.max_entropy),
            workers: available_workers
        });

        // Choose the number of parts to merge
        const L = Math.min(solution.length, this.max_layers);

        // Recompute
        job.entropy = Math.min(this.max_merge_entropy, solution[L - 1]);
        job.target_size = job.bytes / job.parts * Math.exp(job.entropy);

        // job.target_parts = job.bytes / job.target_size;

        // // Make sure we do not assign unnecessary workers
        // job.workers = Math.min(job.plan.available_workers, job.target_parts);
    }

    // Returns true iff `group` should be merged within `job`
    // Also fills the parameters of a merge job
    #strategy(job, group, available_workers) {
        // Always accept the first group
        if (job.children.length == 0) {
            this.#addGroup(job, group, available_workers);
            return true;
        }

        this.#addGroup(job, group, available_workers);
        return true;
    }

    build() {
        const plan = new MergingPlan(this.workers);
        plan.initialize(mt);
        plan.build(() => this.#strategy());
    }
}
