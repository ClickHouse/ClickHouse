// Used to obtain model solutions using interpolation instead of gradient descent
export class ModelSolver {
    constructor(model) {
        this.model = model;
        this.data = [];
    }

    // Solves models in a number of points and store solution for futher interpolation
    async train(minPartSize, maxPartSize, workersMax, method, onProgress) {
        // Feasible ranges for parameters
        let partsMin = 4; // There is no point in merging 4 or less parts using more than 1 merge
        let partsMax = maxPartSize / minPartSize; // Max possible number of parts given constraints
        let workersMin = 1;

        // Prepare to report progress
        let runs_total = 0;
        let runs_done = 0;
        let last_logged = 0;

        // Cumpute number of model runs
        for (let i = 0, parts = partsMin; parts <= partsMax; parts *= 2, i++) {
            for (let j = 0, workers = workersMin; workers <= workersMax; workers *= 2, j++) {
                if (workers <= parts / 2)
                    runs_total++;
            }
        }

        for (let i = 0, parts = partsMin; parts <= partsMax; parts *= 2, i++) {
            if (this.data.length <= i)
                this.data[i] = [];
            let data_i = this.data[i];
            for (let j = 0, workers = workersMin; workers <= workersMax; workers *= 2, j++) {
                if (workers > parts / 2)
                    continue; // too many workers
                if (data_i.length <= j)
                    data_i[j] = [];
                let data_i_j = data_i[j];
                this.model({parts, workers, maxSourceParts: parts, maxPartSize: null, insertPartSize: null},
                    method,
                    function onOption(L, x, Fx) {
                        data_i_j[L] = x[0];
                        console.log({parts, workers, L, Fx, x: x[0]});
                    }
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

    solve({parts, workers, maxSourceParts, maxPartSize, insertPartSize, layers}) {

    }
}
