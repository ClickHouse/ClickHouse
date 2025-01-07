import { gradientDescent } from './gradientDescent.js';

export function mergeModel({insertPartSize, parts, workers, maxSourceParts, maxPartSize}, onOption) {
    // We pretend that we merge initial parts that all have 1-byte size
    const finalSize =
        (maxPartSize == null ?
        parts : // Max part size is unlimited - we merge all parts into one big part
        maxPartSize / insertPartSize);

    const x_sum = Math.log(finalSize);
    const N = workers;
    const yBar = x_sum - Math.log(N);
    const xMax = Math.log(maxSourceParts);

    const B = 1;

    const sumVectors = (vectors) => vectors.reduce((acc, vec) => acc.map((val, index) => val + vec[index]), new Array(vectors[0].length).fill(0));
    const y = (x, i) => x.slice(0, i + 1).reduce((acc, val) => acc + val, 0); // Note that y(x, -1) = 0
    const Iplus  = (x, i) => B * Math.exp(x[i]);
    const Iminus = (x, i) => B/2 * (
        + Math.exp(yBar - y(x, i - 1))
        + Math.exp(yBar - y(x, i))
        + Math.exp(x[i])
        - 1
    );
    const I = (x, i) => (y(x, i) >= yBar ? Iplus(x, i) : Iminus(x, i));
    const F = (x) => x.reduce((a, xi, i) => a + I(x, i), 0);
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
    let eta = 1e-3; // Step size

    for (let L = 1; L <= 1.1 * x_sum; L++) {
        let x = gradientDescent(grad_F, L, x_sum, xMax, eta);
        let Fx = F(x);
        onOption(L, x, Fx);
    }
}
