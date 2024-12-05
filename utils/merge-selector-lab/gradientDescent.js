export function gradientDescent(gradF, L, n, eta, maxIterations = 100000, tolerance = 1e-8) {
    // Initialize variables
    const log_n = Math.log(n);

    function projection(x) {
        // Project onto the feasible set (all x[i] >= 0 and sum(x) = ln(n))
        let sumX = x.reduce((a, b) => a + b, 0);
        let correction = (log_n - sumX) / L;
        let projectedX = x.map(xi => xi + correction);
        let sumProjected = projectedX.reduce((a, b) => a + b, 0);

        // Ensure all x[i] >= 0 and re-adjust if necessary
        if (sumProjected <= log_n) {
            return projectedX.map(xi => Math.max(0, xi));
        }

        // If negative values exist, set them to zero and re-scale to maintain the sum
        let positiveX = projectedX.map(xi => Math.max(0, xi));
        let positiveSum = positiveX.reduce((a, b) => a + b, 0);
        let scale = log_n / positiveSum;

        return positiveX.map(xi => xi * scale);
    }

    // let x = Array(L).fill(log_n / L); // Start with an equal distribution
    let x = projection(Array(L).fill(1).map((xi) => Math.random() * log_n));

    let iteration = 0;
    while (iteration < maxIterations) {
        let grad = gradF(x);

        // Update x with gradient descent step
        let xNew = x.map((xi, i) => xi - eta * grad[i]);

        // Project onto feasible region
        xNew = projection(xNew);

        // Check for convergence
        let diff = xNew.map((xi, i) => Math.abs(xi - x[i])).reduce((a, b) => a + b, 0);
        // console.log("GD ITERATION", {x, grad, xNew, diff});
        if (diff < tolerance)
            break;

        x = xNew;
        iteration++;
    }

    return x;
}
