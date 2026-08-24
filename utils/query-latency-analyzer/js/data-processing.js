/**
 * Computes the statistical info for each group in the input array of objects.
 * @param {Array} raw_data - Array of objects, each containing a "group" property and numerical properties.
 * @returns {Object} - An object containing:
 *   - names: Ordered array of property names (excluding "group").
 *   - groups: An object where keys are group names and values are statistics:
 *     - count: number of items in the group
 *     - cov: covariance matrix
 *     - avg: mean vector
 *     - std: standard deviation vector
 *     - min: minimum values vector
 *     - max: maximum values vector
 *     - p: percentiles objects vector (key: 1 to 99, value: percentile value)
 */
function processData(raw_data) {
    if (!Array.isArray(raw_data) || raw_data.length === 0) {
        return { labels: [], groups: {} };
    }

    // Extract variable names (excluding "group")
    const labels = Object.keys(raw_data[0]).filter(key => key !== "group").sort();

    // Group data by "group" property
    const groupedData = raw_data.reduce((acc, item) => {
        const group = item.group;
        if (!acc[group]) acc[group] = [];
        acc[group].push(item);
        return acc;
    }, {});

    // Compute covariance matrices for each group
    const groups = {};
    for (const [group, items] of Object.entries(groupedData)) {
        const n = items.length;

        // Extract data for each variable
        const columns = labels.map(name => items.map(item => +item[name]));

        const res = {
            count: n,
            cov: Array(labels.length).fill(null).map(() => Array(labels.length).fill(0)),
            avg: Array(labels.length).fill(0),
            std: Array(labels.length).fill(0),
            min: Array(labels.length).fill(Infinity),
            max: Array(labels.length).fill(-Infinity),
            p: Array(labels.length).fill(null).map(() => ({}))
        };

        // Compute average
        for (let k = 0; k < n; k++) {
            for (let i = 0; i < labels.length; i++) {
                res.avg[i] += columns[i][k];
            }
        }
        for (let i = 0; i < labels.length; i++) {
            res.avg[i] /= n;
        }

        // Compute variance and covariance
        for (let k = 0; k < n; k++) {
            for (let i = 0; i < labels.length; i++) {
                const diffI = columns[i][k] - res.avg[i];
                for (let j = 0; j < labels.length; j++) {
                    const diffJ = columns[j][k] - res.avg[j];
                    res.cov[i][j] += diffI * diffJ;
                }
            }
        }
        for (let i = 0; i < labels.length; i++) {
            for (let j = 0; j < labels.length; j++) {
                res.cov[i][j] /= n;
            }
            res.std[i] = Math.sqrt(res.cov[i][i]);
        }

        // Compute min, max, and percentiles
        const percentiles = Array.from({ length: 99 }, (_, i) => i + 1);
        for (let i = 0; i < labels.length; i++) {
            const sortedColumn = [...columns[i]].sort((a, b) => a - b);
            res.min[i] = sortedColumn[0];
            res.max[i] = sortedColumn[sortedColumn.length - 1];

            percentiles.forEach(p => {
                const rank = Math.ceil((p / 100) * (sortedColumn.length - 1));
                res.p[i][p] = sortedColumn[rank];
            });
        }

        groups[group] = res;
    }

    return { labels, groups };
}

/**
 * Generates test data with 3000 objects divided into 3 groups with different sizes.
 * Each object has 8 properties, some independent and others correlated, with different mean values for each group.
 * @returns {Array} - Array of generated test data objects.
 */
function generateTestData() {
    const groups = [
        { name: "Group1", size: 1500, mean: [80, 70, 60, 50, 40, 30, 20, 10] },
        { name: "Group2", size: 1000, mean: [15, 25, 35, 45, 55, 65, 75, 85] },
        { name: "Group3", size: 500, mean: [90, 80, 70, 60, 50, 40, 30, 20] }
    ];

    const data = [];

    groups.forEach(group => {
        for (let i = 0; i < group.size; i++) {
            const obj = { group: group.name };
            obj._sum = 0;

            // Generate independent properties
            for (let j = 0; j < 4; j++) {
                obj._sum += obj[`prop${j + 1}`] = Math.max(0, group.mean[j] + (Math.random() - 0.5) * 20 * j);
            }

            // Generate correlated properties
            for (let j = 4; j < 8; j++) {
                obj._sum += obj[`prop${j + 1}`] = Math.max(0, obj[`prop${j - 4 + 1}`] * 1.5 + (Math.random() - 0.5) * 10 * j);
            }

            data.push(obj);
        }
    });

    return data;
}

/**
 * Reorders labels and corresponding data according to a comparison function
 * @param {Object} data - Object containing labels array and groups object from processData
 * @param {Function} comp - Comparison function for sorting labels
 * @returns {Object} - Object with the same structure but reordered according to comp
 */
function orderLabels({ labels, groups }, comp) {
    // Create array of indices and sort it according to how labels should be sorted
    const indices = labels.map((_, i) => i);
    indices.sort((a, b) => comp(labels[a], labels[b]));

    // Create new labels array in the desired order
    const newLabels = indices.map(i => labels[i]);

    // Create new groups object with reordered arrays
    const newGroups = {};
    for (const [groupName, groupData] of Object.entries(groups)) {
        const { count } = groupData;
        newGroups[groupName] = {
            count,
            // Reorder covariance matrix - need to reorder both rows and columns
            cov: indices.map(i => indices.map(j => groupData.cov[i][j])),
            // Reorder 1D arrays
            avg: indices.map(i => groupData.avg[i]),
            std: indices.map(i => groupData.std[i]),
            min: indices.map(i => groupData.min[i]),
            max: indices.map(i => groupData.max[i]),
            // Reorder percentiles array
            p: indices.map(i => groupData.p[i])
        };
    }

    return { labels: newLabels, groups: newGroups };
}

/**
 * Computes global averages across all groups weighted by group size
 * @param {Object} data - Object containing labels array and groups object from processData
 * @returns {Array} - Array of global averages corresponding to labels
 */
function computeGlobalAverages({ labels, groups }) {
    const globalAvg = new Array(labels.length).fill(0);
    let totalCount = 0;

    // Sum up weighted averages and total count
    for (const groupData of Object.values(groups)) {
        for (let i = 0; i < labels.length; i++) {
            globalAvg[i] += groupData.count * groupData.avg[i];
        }
        totalCount += groupData.count;
    }

    // Normalize by total count
    if (totalCount > 0) {
        for (let i = 0; i < globalAvg.length; i++) {
            globalAvg[i] /= totalCount;
        }
    }

    return globalAvg;
}

/**
 * Filters labels and corresponding data according to a predicate function
 * @param {Object} data - Object containing labels array and groups object from processData
 * @param {Function} predicate - Function that takes label and its data and returns true if label should be kept
 * @returns {Object} - Filtered object with the same structure
 */
function filterData({ labels, groups }, predicate) {
    // Create array of indices for labels that pass the predicate
    const indices = labels.map((label, i) => ({label, i}))
        .filter(({label, i}) => predicate(label, i, groups))
        .map(({i}) => i);

    // Create new labels array with only filtered labels
    const newLabels = indices.map(i => labels[i]);

    // Create new groups object with filtered arrays
    const newGroups = {};
    for (const [groupName, groupData] of Object.entries(groups)) {
        const { count } = groupData;
        newGroups[groupName] = {
            count,
            // Filter covariance matrix
            cov: indices.map(i => indices.map(j => groupData.cov[i][j])),
            // Filter 1D arrays
            avg: indices.map(i => groupData.avg[i]),
            std: indices.map(i => groupData.std[i]),
            min: indices.map(i => groupData.min[i]),
            max: indices.map(i => groupData.max[i]),
            // Filter percentiles array
            p: indices.map(i => groupData.p[i])
        };
    }

    return { labels: newLabels, groups: newGroups };
}

// Make sure to expose functions if using modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { processData, generateTestData, orderLabels, computeGlobalAverages, filterData };
} else {
    // For browser environment
    window.processData = processData;
    window.generateTestData = generateTestData;
    window.orderLabels = orderLabels;
    window.computeGlobalAverages = computeGlobalAverages;
    window.filterData = filterData;
}
