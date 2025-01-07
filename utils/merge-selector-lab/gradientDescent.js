export function gradientDescent(gradF, L, x_sum, xMax, eta, maxIterations = 100000, tolerance = 1e-8) {
    function projection(x_input) {
        // Project onto the feasible set:
        // 1) sum(x) = x_sum # equals ln(n) if part size is unlimited
        // 2) all x[i] >= 0
        // 3) all x[i] <= xMax

        if (L == 1)
            return [x_sum]; // The only feasible point

        // Project onto `sum(x) = x_sum` plane (1)
        let delta = (x_input.reduce((a, xi) => a + xi, 0) - x_sum) / L;
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
    let x = projection(Array(L).fill(1).map((xi) => Math.random() * x_sum));

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
