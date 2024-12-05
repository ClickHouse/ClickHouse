export function gradientDescent(gradF, L, n, xMax, eta, maxIterations = 100000, tolerance = 1e-8) {
    // Initialize variables
    const log_n = Math.log(n);

    function projection(x_input) {
        // Project onto the feasible set:
        // 1) sum(x) = ln(n)
        // 2) all x[i] >= 0
        // 3) all x[i] <= xMax

        if (L == 1)
            return [log_n]; // The only feasible point

        // Project onto `sum(x) = ln(n)` plane (1)
        let delta = (x_input.reduce((a, xi) => a + xi, 0) - log_n) / L;
        let x = x_input.map((xi) => xi - delta);

        // Take (2) and (3) into account, while staying in plane (1)
        let correction = 0;
        let i = 0;
        for (; i < 100; i++) {
            let correction_sum = 0;
            for (let i = 0; i < L; i++) {
                let value = x[i] + correction; // projects onto (1)
                let new_value = Math.max(0, Math.min(xMax, value));
                correction_sum += value - new_value;
                x[i] = new_value;
            }
            correction = correction_sum / L;
            if (correction < 1e-10)
                break;
        }
        return x;
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
