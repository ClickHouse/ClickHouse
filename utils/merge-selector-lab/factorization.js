function factorizeNumber(n, start = 1)
{
    const factors = [];

    // Iterate through possible factor pairs
    for (let i = start; i <= Math.sqrt(n); i++)
    {
        if (n % i === 0)
        {
            const factor1 = i;
            const factor2 = n / i;
            if (factor1 !== 1)
            { // Exclude factorization involving 1
                factors.push([factor1, factor2]);
            }

            // Recurse into the second factor to generate further factorizations, ensuring no infinite recursion
            if (factor2 > factor1 && factor2 !== n && factor1 !== 1)
            {  // Prevent immediate and trivial self-references
                const subFactors = factorizeNumber(factor2, factor1);
                subFactors.forEach(subFactor => {
                    factors.push([factor1, ...subFactor]);
                });
            }
        }
    }

    return factors;
}

function allFactorPermutations(factorizations)
{
    const allPermutations = [];

    // Helper function to generate permutations
    function permute(arr, l, r)
    {
        if (l === r)
        {
            allPermutations.push([...arr]);
        }
        else
        {
            for (let i = l; i <= r; i++)
            {
                [arr[l], arr[i]] = [arr[i], arr[l]]; // Swap
                permute(arr, l + 1, r);
                [arr[l], arr[i]] = [arr[i], arr[l]]; // Swap back
            }
        }
    }

    factorizations.forEach(factorSet =>
    {
        permute(factorSet, 0, factorSet.length - 1);
    });

    // Remove duplicates and sort lexicographically
    const uniquePermutations = Array.from(new Set(allPermutations.map(JSON.stringify))).map(JSON.parse);
    uniquePermutations.sort((a, b) =>
    {
        for (let i = 0; i < Math.min(a.length, b.length); i++)
        {
            if (a[i] !== b[i])
            {
                return a[i] - b[i];
            }
        }
        return a.length - b.length;
    });

    return uniquePermutations;
}

export { factorizeNumber, allFactorPermutations };

// Example Usage
// import { factorizeNumber, allFactorPermutations } from './factorization.js';

// const number = 100;
// const factorCombinations = factorizeNumber(number);
// const perm = allFactorPermutations(factorCombinations);

// perm.forEach(factorSet =>
// {
//     console.log(`Permutation: ${factorSet.join(' x ')} = ${number}`);
// });
