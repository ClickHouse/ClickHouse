/**
 * Projection onto the feasible set:
 *   1) sum(x) = xSum
 *   2) x[i] >= 0
 *   3) x[i] <= xMax
 */
export function defaultProjection(x, L, xSum, xMax) {
    // If L == 1, only feasible solution is [xSum].
    if (L == 1)
        return [xSum];

    // Project onto the plane sum(x) = xSum
    let delta = (x.reduce((a, xi) => a + xi, 0) - xSum) / L;
    let xProjected = x.map((xi) => xi - delta);

    // Respect 0 <= x_i <= xMax, while keeping sum(x) = xSum
    let correction = 0;
    for (let iter = 0; iter < 100; iter++) {
        let correctionSum = 0;
        for (let i = 0; i < L; i++) {
            let value = xProjected[i] + correction;
            let newValue = Math.max(0, Math.min(xMax, value));
            correctionSum += (value - newValue);
            xProjected[i] = newValue;
        }
        correction = correctionSum / L;
        if (Math.abs(correction) < 1e-10) break;
    }
    return xProjected;
}

/**
 * Orthogonal projection of x onto the hyperplane sum(x) = xSum.
 * Ignores all other constraints (like 0 <= x[i] <= xMax).
 */
export function orthogonalProjection(x, L, xSum, xMax) {
    // If L == 1, the only feasible solution is [xSum].
    if (L == 1)
        return [xSum];

    // sum(x) currently
    const currentSum = x.reduce((acc, xi) => acc + xi, 0);

    // The amount each element has to shift by to correct the sum.
    // (sum(x) - xSum)/L
    const delta = (currentSum - xSum) / L;

    // Project by subtracting delta from each coordinate
    return x.map((xi) => xi - delta);
}

function initialGuess(L, xSum) {
    return Array(L).fill(xSum / L); // Start with an equal distribution
    // return projection(Array(L).fill(1).map((xi) => Math.random() * xSum));
}

/**
 * Method for Smooth Optimization
 */
export function projectedGradientDescent(
    gradF,           // function that computes the gradient given x
    L,               // dimension of x
    xSum,            // sum(x[i]) constraint
    xMax,            // maximum bound for each x[i]
    xInit = initialGuess(L, xSum),
    eta = 1e-3,      // step size
    projFn = defaultProjection,
    maxIterations = 100000,
    tolerance = 1e-5,
) {
    const projection = (x) => projFn(x, L, xSum, xMax);
    let x = xInit;

    // Check unfeasibility
    if (xMax * L <= xSum)
        return x;

    for (let iteration = 1; iteration <= maxIterations; iteration++) {
        let grad = gradF(x);

        // Update x with gradient descent step
        let xNew = x.map((xi, i) => xi - eta * grad[i]);

        // Project onto feasible region
        xNew = projection(xNew);

        // Check for convergence
        let diff = xNew.reduce((acc, xi, i) => acc + Math.abs(xi - x[i]), 0);
        // if (iteration % 10000 == 0) console.log("GD ITERATION", iteration, {x, grad, xNew, diff});
        if (diff < tolerance) {
            // console.log(`Iterations: ${iteration}`);
            return x;
        }

        x = xNew;
    }

    throw { message: "Gradient descent does not converge:", L, xSum, xMax, eta, projFn, maxIterations, tolerance};
}

/**
 * Faster Method for Smooth Optimization
 */
export function nesterovAcceleratedPGD(
    gradF,           // function that computes the gradient given x
    L,               // dimension of x
    xSum,            // sum(x[i]) constraint
    xMax,            // maximum bound for each x[i]
    xInit = initialGuess(L, xSum),
    eta = 1e-3,      // step size
    projFn = defaultProjection,
    maxIterations = 100000,
    tolerance = 1e-5,
    momentum = 0.9,  // typical momentum values in [0.7, 0.99]
) {
    const projection = (x) => projFn(x, L, xSum, xMax);
    let x = xInit;

    // Check unfeasibility
    if (xMax * L <= xSum)
        return x;

    // Momentum vector (velocity)
    let v = Array(L).fill(0);

    for (let iteration = 1; iteration <= maxIterations; iteration++) {
        // 1) "Lookahead": y = x - momentum * v
        let y = x.map((xi, i) => xi - momentum * v[i]);

        // 2) Compute gradient at y
        let grad = gradF(y);

        // 3) Update velocity: v_new = momentum * v + eta * gradF(y)
        let vNew = v.map((vi, i) => momentum * vi + eta * grad[i]);

        // 4) Tentative x update: x_new = x - v_new
        let xNew = x.map((xi, i) => xi - vNew[i]);

        // 5) Project back to the feasible region
        xNew = projection(xNew);

        // 6) Check convergence
        let diff = xNew.reduce((acc, xi, i) => acc + Math.abs(xi - x[i]), 0);
        // if (iteration % 10000 == 0) console.log("GD ITERATION", iteration, {x, grad, xNew, diff});
        if (diff < tolerance) {
            // console.log(`Iterations: ${iteration}`);
            return x;
        }

        // 7) Update x and v
        x = xNew;
        v = xNew.map((xi, i) => xi - (x[i] + vNew[i] - xNew[i]));
        // or, more simply, we can re-define v directly from x changes:
        // v = xNew.map((xi, i) => xi - xOld[i]); // but be consistent with the formula

        v = xNew.map((xi, i) => x[i] - xi);
        // The typical Nesterov formula sets v_k = x_k - x_{k+1} (depending on sign).
        // You can choose either approach as long as it is consistent.
    }
    throw { message: "Gradient descent does not converge:", L, xSum, xMax, eta, projFn, maxIterations, tolerance};
}

/**
 * Subgradient Method for Non-Smooth Optimization
 */
export function subgradientMethod(
    gradF,           // function that computes any gradient in subgradient for given x
    L,               // dimension of x
    xSum,            // sum(x[i]) constraint
    xMax,            // maximum bound for each x[i]
    xInit = initialGuess(L, xSum),
    alpha = 1e-1,    // initial step size
    projFn = defaultProjection,
    maxIterations = 1000000,
    tolerance = 1e-6,
) {
    const projection = (x) => projFn(x, L, xSum, xMax);
    let x = xInit;

    // Check unfeasibility
    if (xMax * L <= xSum)
        return x;

    // Compute optimal step size
    // Assume reasonable region for an optimal solution to be x[i] >= 1, with sum(x) = xSum
    const xMaxAdj = Math.max(1, xSum - L);
    const R = Math.sqrt(2) * xMaxAdj;
    let G = 0;
    for (let i = 0; i < L; i++) {
        // Compute max values for a gradient
        let v = Array(L).fill(1);
        v[i] = xMaxAdj;
        const grad = gradF(v);
        const gradNorm = Math.sqrt(grad.reduce((acc, gi) => acc + gi * gi, 0));
        if (G < gradNorm)
            G = gradNorm;
    }
    alpha = R / G;
    // console.log("OPT STEP", {xMaxAdj, R, G, alpha});

    let xSumAvg = Array(L).fill(0);
    let prevXAveraged = Array(L).fill(0);
    for (let iteration = 1; iteration <= maxIterations; iteration++) {
        // Compute subgradient at current x
        let grad = gradF(x);

        // Compute step size: eta_k = alpha / sqrt(k)
        let eta = alpha / Math.sqrt(iteration);

        // Update x: x_new = x - eta * g
        let xNew = x.map((xi, i) => xi - eta * grad[i]);

        // Project back to feasible set
        xNew = projection(xNew, L, xSum, xMax);

        // Update averaging variables
        xSumAvg = xSumAvg.map((sum, i) => sum + xNew[i]);

        // Compute averaged iterate
        let xAveraged = xSumAvg.map(sum => sum / iteration);

        // Check for convergence
        let diff = xAveraged.reduce((acc, xi, i) => acc + Math.abs(xi - prevXAveraged[i]), 0);
        // if (iteration % 10000 == 0) console.log("GD ITERATION", iteration, {x, grad, xNew, xAveraged, diff});
        if (diff < tolerance) {
            // console.log(`Iterations: ${iteration}`);
            return xAveraged;
        }

        // Updates
        prevXAveraged = xAveraged;
        x = xNew;
    }
    throw { message: "Subgradient method does not converge:", L, xSum, xMax, alpha, projFn, maxIterations, tolerance};
}
