// Used to obtain model solutions using interpolation instead of gradient descent
export class ModelSolver {
    constructor(model) {
        this.model = model;
        this.data = [];
        this.partsMin = 4; // There is no point in merging 4 or less parts using more than 1 merge
        this.partsMax = null; // Max possible number of parts given constraints
        this.workersMin = 1;
        this.workersMax = null;
        this.minRatioExp = Math.log2(this.partsMin / this.workersMin);
        this.partsStep = 1;
        this.workersStep = 1;
    }

    // Returns solutions for arbitrary i given unlimited worker pool
    #getSolutionsForUnlimitedWorkers(parts) {
        // This problem is much simpler and does not require a solver
        // Analytical solution -- merge the same number of parts on all layers:
        //     x[i] = xSum / L  for all i=0..L-1
        // Optimal number of layers Lopt is given by floor(xSum) expression
        // Thus, with unlimited workers optimal strategy is to merge ~e parts
        const xSum = Math.log(parts);
        const Lopt = Math.floor(xSum);
        let solutions = [];
        for (let L = 1; L <= Lopt + 1; L++)
            solutions.push(xSum / L);
        return solutions;
    }

    // Returns solutions for integer i and j
    #getSolutions(i, j) {
        const xSum = Math.log(this.partsMin) + i * this.partsStep * Math.LN2;
        if (i < 0) // There are too few parts -- merge at once
            return [xSum];
        if (this.minRatioExp < j * this.workersStep - i * this.partsStep + 1) // Equivalent to `workers > parts / 2`
            return this.#getSolutionsForUnlimitedWorkers(this.partsMin * Math.pow(2, i * this.partsStep));
        if (i < this.data.length) {
            const data_i = this.data[i];
            if (j < data_i.length)
                return [xSum, ...data_i[j]];
        }
        throw {
            message: "parts or workers are out of range for solver",
            parts: this.partsMin * Math.pow(2, i * this.partsStep),
            workers: this.workersMin * Math.pow(2, j * this.workersStep),
            partsMax: this.partsMax,
            workersMax: this.workersMax,
        };
    }

    // Estimate the best solution for given parameters using training data
    // Returns array of x[0] values for L = 1..Lopt values,
    // representing optimal number of parts to merge for L-layered plan
    // Length of returned array equals optimal L (number of layers) to minimize objective function
    solve({parts, workers, maxSourceParts, maxPartSize, insertPartSize}) {
        if (workers > parts / 2)
            return this.#getSolutionsForUnlimitedWorkers(parts);

        // We pretend that we merge initial parts that all have 1-byte size
        const finalSize =
            (maxPartSize == null ?
            parts : // Max part size is unlimited - we merge all parts into one big part
            maxPartSize / insertPartSize);

        // TODO(serxa): Constraints
        const xSum = Math.log(finalSize);
        const xMax = Math.log(maxSourceParts);

        // Find points for interpolation
        const i = Math.log2(finalSize / this.partsMin) / this.partsStep;
        const j = Math.log2(workers / this.workersMin) / this.workersStep;
        const i0 = Math.floor(i);
        const j0 = Math.floor(j);
        const i1 = i0 + 1;
        const j1 = j0 + 1;

        // Get precomputed solutions at nearest points
        const x00 = this.#getSolutions(i0, j0);
        const x01 = this.#getSolutions(i0, j1);
        const x10 = this.#getSolutions(i1, j0);
        const x11 = this.#getSolutions(i1, j1);

        // Make arrays to have the same size, by replicating last elements
        // This is justified by empirical fact that x[0] for L>Lopt is similar to x[0] for Lopt
        let len = Math.max(x00.length, x01.length, x10.length, x11.length);
        while (x00.length < len)
            x00.push(x00[x00.length - 1]);
        while (x01.length < len)
            x01.push(x01[x01.length - 1]);
        while (x10.length < len)
            x10.push(x10[x10.length - 1]);
        while (x11.length < len)
            x11.push(x11[x11.length - 1]);

        // Bilinear interpolation coefficients
        const ci1 = i - i0;
        const ci0 = 1 - ci1;
        const cj1 = j - j0;
        const cj0 = 1 - cj1;
        const c00 = ci0 * cj0;
        const c10 = ci1 * cj0;
        const c01 = ci0 * cj1;
        const c11 = ci1 * cj1;

        // Bilinear interpolation
        let result = [];
        for (let k = 0; k < len; k++) {
            result.push(c00 * x00[k] + c10 * x10[k] + c01 * x01[k] + c11 * x11[k]);
        }
        return result;
    }

    // Objective function for lowest layer given x[0] value
    F0(x0, parts, workers) {
        const n0 = Math.exp(x0);
        const merges = parts / n0;
        const alpha0 = Math.max(1, merges / workers);
        return 0.5 * (
             (alpha0 + 1) * n0
            + alpha0 - 1
        );
    }

    // Imitates behaviour of `mergeModel` function from 'mergeModel.js'
    mergeModel({parts, workers, maxSourceParts, maxPartSize, insertPartSize}, onOption) {
        // We pretend that we merge initial parts that all have 1-byte size
        const finalSize =
            (maxPartSize == null ?
            parts : // Max part size is unlimited - we merge all parts into one big part
            maxPartSize / insertPartSize);

        const xSum = Math.log(finalSize);
        const xMax = Math.log(maxSourceParts);

        const solutions = this.solve({
            parts,
            workers,
            maxSourceParts,
            maxPartSize,
            insertPartSize
        });
        for (let k = 0; k < solutions.length; k++) {
            const L = k + 1;

            // Check feasibility
            if (xMax * L < xSum)
                continue;

            let partsLeft = finalSize;
            let solution = solutions[k];
            let x = [];
            let Fx = 0; // Objective function

            // We only store x[0] values, and to restore x[1]..x[L-1] values we recursively solve smaller subproblems
            for (let l = L; l >= 1;) {
                let xi = Math.min(xMax, solution);
                x.push(xi);
                Fx += this.F0(xi, partsLeft, workers);
                partsLeft /= Math.exp(xi);
                l--;

                if (l != 0) {
                    let res = this.solve({
                        parts: partsLeft,
                        workers,
                        maxSourceParts,
                        maxPartSize,
                        insertPartSize
                    });
                    if (l > res.length)
                        break; // Solution is not optimal
                    solution = res[l - 1];
                }
            }
            if (x.length == L)
                onOption(L, x, Fx);
        }
    }

    // Solves models in a number of points and store solution for further interpolation
    async train(minPartSize, maxPartSize, workersMax, partsStep, workersStep, method, onProgress) {
        // Feasible ranges for parameters
        this.partsMax = maxPartSize / minPartSize;
        this.workersMax = workersMax;

        // Steps
        this.partsStep = partsStep;
        this.workersStep = workersStep;
        const partsMult = Math.pow(2, this.partsStep);
        const workersMult = Math.pow(2, this.workersStep);

        // Prepare to report progress
        let runs_total = 0;
        let runs_done = 0;
        let last_logged = 0;

        // Cumpute number of model runs
        for (let i = 0, parts = this.partsMin; parts <= this.partsMax; parts *= partsMult, i++) {
            for (let j = 0, workers = this.workersMin; workers <= this.workersMax; workers *= workersMult, j++) {
                if (workers <= parts / 2)
                    runs_total++;
            }
        }

        for (let i = 0, parts = this.partsMin; parts <= this.partsMax; parts *= partsMult, i++) {
            if (this.data.length <= i)
                this.data[i] = [];
            let data_i = this.data[i];
            let xInit = {}; // Cache: reuse previous solutions as initial guess for the next model run
            for (let j = 0, workers = this.workersMin; workers <= this.workersMax; workers *= workersMult, j++) {
                if (workers > parts / 2)
                    continue; // too many workers
                if (data_i.length <= j)
                    data_i[j] = [];
                let data_i_j = data_i[j];
                this.model({parts, workers, maxSourceParts: parts, maxPartSize: null, insertPartSize: null},
                    method,
                    function onOption(L, x, Fx) {
                        // We don't save value for L=1, it's always equal to log(parts)
                        if (L == 1)
                            return;
                        data_i_j[L - 2] = x[0];
                        console.log({parts, workers, L, Fx, x: x[0]});
                    },
                    xInit
                );
                runs_done++;
                const progress = Math.floor(runs_done * 100 / runs_total);
                if (last_logged != progress) {
                    console.log(`TRAINING PROGRESS -- ${progress}% -- ${runs_done}/${runs_total}`);
                    last_logged = progress;
                }
                await onProgress({progress, runs_done, runs_total});
            }
        }
        console.log(`TRAINING DONE -- ${runs_done}/${runs_total}`);
    }

    // Serialize the solver to a JSON string
    serializeData() {
        return JSON.stringify({
            partsMax: this.partsMax,
            workersMax: this.workersMax,
            partsStep: this.partsStep,
            workersStep: this.workersStep,
            data: this.data,
        });
    }

    // Deserialize a JSON string into the solver
    deserializeData(jsonString) {
        try {
            const parsed = JSON.parse(jsonString);
            this.partsMax = parsed.partsMax;
            this.workersMax = parsed.workersMax;
            this.partsStep = parsed.partsStep ?? 1;
            this.workersStep = parsed.workersStep ?? 1;
            this.data = parsed.data;
        } catch (error) {
            throw { message: "Failed to deserialize solver", error };
        }
    }
}
