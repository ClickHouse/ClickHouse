export function mergeModel({parts, workers, maxSourceParts, maxPartSize, insertPartSize}, method, onOption, xInit = {}) {
    // Total number of bytes in all parts (just to keep track of proper units)
    const B = 1;

    // We pretend that we merge initial parts that all have 1-byte size
    const finalSize =
        (maxPartSize == null ?
        parts : // Max part size is unlimited - we merge all parts into one big part
        maxPartSize / insertPartSize);

    // Constraints
    const xSum = Math.log(finalSize);
    const xMax = Math.log(maxSourceParts);

    // Total number of available workers in the pool
    const N = workers;
    const yBar = xSum - Math.log(N);

    const sumVectors = (vectors) => vectors.reduce((acc, vec) => acc.map((val, index) => val + vec[index]), new Array(vectors[0].length).fill(0));

    // Auxiliary y-vector expressed as a function of x-vector:
    // y[i] = sum(x[k], k=0..i), note that y[-1] = 0
    const y = (x, i) => x.slice(0, i + 1).reduce((acc, val) => acc + val, 0); // Note that y(x, -1) = 0

    // Objective functions for layer below and above yBar
    const Iplus  = (x, i) => B * Math.exp(x[i]);
    const Iminus = (x, i) => B/2 * (
        + Math.exp(yBar - y(x, i - 1))
        + Math.exp(yBar - y(x, i))
        + Math.exp(x[i])
        - 1
    );

    // Total objective function is a sum over all layers
    const I = (x, i) => (y(x, i) >= yBar ? Iplus(x, i) : Iminus(x, i));
    const F = (x) => x.reduce((a, xi, i) => a + I(x, i), 0);

    // Corresponding gradients
    const grad_Iplus  = (x, i) => x.map(
        (xj, j) => (i == j ? B * Math.exp(xj) : 0)
    );
    const grad_Iminus = (x, i) => x.map(
        (xj, j) => B/2 * (
            /* grad y[i-1] term */ - (j <  i ? Math.exp(yBar - y(x, i - 1)) : 0)
            /* grad y[i] term */   - (j <= i ? Math.exp(yBar - y(x, i)) : 0)
            /* grad x[i] term */   + (j == i ? Math.exp(xj) : 0)
        )
    );
    const grad_I = (x, i) => (y(x, i) >= yBar ? grad_Iplus(x, i) : grad_Iminus(x, i));
    const grad_F = (x) => sumVectors(x.map((xi, i) => grad_I(x, i)));

    // TODO: determine step size according to inverse of maximum grad_F
    // let eta = 1e-3; // Step size

    // Iterate over all reasonable numbers of layers L
    // It's not obvious but x[i] < 1 cannot be optimal,
    // because there will be a batter F for lower L, so L > xSum are not interesting
    let prevFx = Infinity;
    for (let L = 1; L <= 1.1 * xSum; L++) {
        // Skip unfeasible problems
        if (xMax * L < xSum)
            continue;

        if (!(L in xInit))
            xInit[L] = Array(L).fill(xSum / L);
        let x = method(grad_F, L, xSum, xMax, xInit[L]);
        xInit[L] = x; // We save solution back to xInit to be reused in subsequent calls
        let Fx = F(x);

        onOption(L, x, Fx);

        // Stop if Fx begins to increase, this increase both write amplification and avg part count
        if (Fx > prevFx)
            return;
        prevFx = Fx;
    }
}
