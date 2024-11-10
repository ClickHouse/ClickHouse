export function promiseCombine(promises, combiner, pinger = null)
{
    return new Promise((resolve, reject) =>
    {
        // Wait for all promises in the array to resolve
        Promise.all(promises)
            .then(results =>
            {
                if (pinger)
                    pinger();
                // Call the combiner function with the resolved values
                return combiner(results);
            })
            .then(combinedResult =>
            {
                if (pinger)
                    pinger();
                // Resolve the merged result
                resolve(combinedResult);
            })
            .catch(error =>
            {
                if (pinger)
                    pinger();
                // Reject in case of error
                reject(error);
            });
    });
}

// Example usage
// const promises = [
//     Promise.resolve(2),
//     Promise.resolve(3),
//     Promise.resolve(5)
// ];

// const combiner = (results) =>
// {
//     // For example, return a new Promise that resolves to the sum of the array
//     return new Promise((resolve) =>
//     {
//         const sum = results.reduce((acc, val) => acc + val, 0);
//         resolve(sum);
//     });
// };

// promiseCombine(promises, combiner)
//     .then(result =>
//     {
//         console.log(result); // Output: 10
//     })
//     .catch(error =>
//     {
//         console.error('Error:', error);
//     });
