const colorScale = d3.scaleLinear()
    .domain([-1, -0.5, 0, 0.2, 0.4, 0.6, 0.8, 1])
    .range(["black", "blue", "white", "lightgreen", "yellow", "orange", "red", "violet"]);

function drawLegend(containerId) {
    const margin = { top: 10, right: 50, bottom: 30, left: 10 };
    const container = d3.select(containerId);
    const width = container.node().getBoundingClientRect().width - margin.left - margin.right;
    const height = 20;
    const svg = container.append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .attr("class", "matrix-legend")
        .append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);

    const scale = d3.scaleLinear().domain([-1, 1]).range([0, width]);
    const axis = d3.axisBottom(scale).ticks(9).tickFormat(d => `${(d * 100).toFixed(0)}%`);
    const steps = d3.range(-1, 1, 2 / width);

    svg.selectAll(".legend-cell")
        .data(steps)
        .enter().append("rect")
        .attr("x", d => scale(d))
        .attr("y", 0)
        .attr("width", width / steps.length)
        .attr("height", height)
        .style("fill", d => colorScale(d));

    svg.append("g")
        .attr("transform", `translate(0, ${height})`)
        .call(axis);
}

/**
 * Renders a covariance matrix heatmap with distribution bands and tooltips.
 * @param {string} containerId - Selector for the container element.
 * @param {Array<string>} labels - Array of labels for each matrix row/column.
 * @param {Object} data - Data object containing:
 *   cov: covariance matrix (2D array),
 *   avg: mean vector (Array),
 *   std: standard deviation vector (Array),
 *   min: minimum values vector (Array),
 *   max: maximum values vector (Array),
 *   p: percentiles array of maps (Array of Objects keyed by percentile)
 */
function addVisualization(containerId, labels, { cov, avg, std, min, max, p }) {
    const container = d3.select(containerId);
    const n = cov.length;
    const cellSize = 20;
    const axisLabelOffset = 120;
    const matrixSize = cellSize * n;
    const bandMargin = 20;
    const bandAreaWidth = container.node().getBoundingClientRect().width - matrixSize - bandMargin - axisLabelOffset;
    const margin = {
        top: 2,
        right: axisLabelOffset + bandAreaWidth + bandMargin,
        bottom: 2,
        left: 2
    };

    // Split labels into underscore and non-underscore groups
    const underscoreLabels = labels.filter(l => l.startsWith('_'));
    const normalLabels = labels.filter(l => !l.startsWith('_'));

    // Create a mapping from label to y-position with gap
    const gapSize = cellSize / 2;
    const labelToY = new Map();

    underscoreLabels.forEach((label, i) => {
        labelToY.set(label, i * cellSize);
    });

    normalLabels.forEach((label, i) => {
        labelToY.set(label, (i + underscoreLabels.length) * cellSize + gapSize);
    });

    const totalHeight = matrixSize + (underscoreLabels.length > 0 ? gapSize : 0);

    const svg = container.append("svg")
        .attr("width", matrixSize + margin.left + margin.right)
        .attr("height", totalHeight + margin.top + margin.bottom)
        .attr("class", "matrix")
        .append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);

    // Compute sum of elements in covariance matrix, excluding rows and columns where labels start with underscore
    let total = 0;
    for (let i = 0; i < n; i++) {
        if (!labels[i].startsWith("_")) {
            for (let j = 0; j < n; j++) {
                if (!labels[j].startsWith("_")) {
                    total += cov[i][j];
                }
            }
        }
    }
    if (total === 0) total = 1;

    const norm = cov.map(row => row.map(v => v / total));
    for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
            if (i < j) {
                norm[i][j] += norm[j][i];
                norm[j][i] = null;
            }
        }
    }

    // Tooltip for matrix and distributions
    const tooltip = d3.select(".matrix-tooltip");

    // Draw matrix cells with adjusted positions
    svg.selectAll(".cell")
        .data(norm.flatMap((row, i) => row.map((v, j) => ({ i, j, v, label: labels[i] }))).filter(d => d.v != null))
        .enter().append("rect")
        .attr("class", "cell")
        .attr("x", d => d.j * cellSize)
        .attr("y", d => labelToY.get(d.label))
        .attr("width", cellSize)
        .attr("height", cellSize)
        .style("fill", d => colorScale(d.v))
        .on("mouseover", (event, d) => {
            tooltip.style("visibility", "visible")
                .text(`${labels[d.i]} × ${labels[d.j]}: ${(d.v * 100).toFixed(2)}%`);
        })
        .on("mousemove", event => {
            tooltip.style("top", `${event.pageY - 10}px`)
                .style("left", `${event.pageX + 10}px`);
        })
        .on("mouseout", () => tooltip.style("visibility", "hidden"));

    // Y-axis with gap
    const yScale = d3.scaleOrdinal()
        .domain([...underscoreLabels.map(label => label.substring(1)), ...normalLabels])
        .range(labels.map(label => labelToY.get(label) + cellSize / 2));

    svg.append("g")
        .attr("class", "axis")
        .attr("transform", `translate(${matrixSize},0)`)
        .call(d3.axisRight(yScale))
        .selectAll("text")
        .on("mouseover", (event, d) => {
            const idx = labels.indexOf(d);
            svg.selectAll(".cell").classed("unlight", c => c.i !== idx && c.j !== idx);
        })
        .on("mouseout", () => svg.selectAll(".cell").classed("unlight", false));

    // Distribution bands to the right of matrix per variable
    const xScale = d3.scaleLinear()
        .domain([d3.min(min), d3.max(max)])
        .range([0, bandAreaWidth]);

    labels.forEach((label, i) => {
        const y = labelToY.get(label);
        const percMap = p[i];
        const percentileKeys = Object.keys(percMap).map(Number).sort((a, b) => a - b);

        percentileKeys.forEach((perc, idx) => {
            const val = percMap[perc];
            const nextVal = idx < percentileKeys.length - 1 ? percMap[percentileKeys[idx + 1]] : max[i];
            const x0 = matrixSize + axisLabelOffset + xScale(val);
            const x1 = matrixSize + axisLabelOffset + xScale(nextVal);
            svg.append("rect")
                .attr("x", Math.floor(x0))
                .attr("y", y)
                .attr("width", Math.ceil(x1 - x0))
                .attr("height", cellSize - 1)
                .style("fill", colorScale(perc / 100))
                .on("mouseover", (event) => {
                    tooltip.style("visibility", "visible")
                        .text(`${label} ${perc}th percentile: ${val.toFixed(2)}`);
                })
                .on("mousemove", event => {
                    tooltip.style("top", `${event.pageY - 10}px`)
                        .style("left", `${event.pageX + 10}px`);
                })
                .on("mouseout", () => tooltip.style("visibility", "hidden"));
        });

        // Std bar with adjusted position
        svg.append("line")
            .attr("x1", matrixSize + axisLabelOffset + xScale(avg[i] - std[i]))
            .attr("x2", matrixSize + axisLabelOffset + xScale(avg[i] + std[i]))
            .attr("y1", y + cellSize / 2)
            .attr("y2", y + cellSize / 2)
            .style("stroke", "black")
            .on("mouseover", () => {
                tooltip.style("visibility", "visible")
                    .text(`${label} ±1σ: [${(avg[i]-std[i]).toFixed(2)}, ${(avg[i]+std[i]).toFixed(2)}]`);
            })
            .on("mousemove", event => {
                tooltip.style("top", `${event.pageY - 10}px`)
                    .style("left", `${event.pageX + 10}px`);
            })
            .on("mouseout", () => tooltip.style("visibility", "hidden"));

        // Mean point with adjusted position
        svg.append("circle")
            .attr("cx", matrixSize + axisLabelOffset + xScale(avg[i]))
            .attr("cy", y + cellSize / 2)
            .attr("r", 2)
            .style("fill", "black")
            .on("mouseover", () => {
                tooltip.style("visibility", "visible")
                    .text(`${label} mean: ${avg[i].toFixed(2)}`);
            })
            .on("mousemove", event => {
                tooltip.style("top", `${event.pageY - 10}px`)
                    .style("left", `${event.pageX + 10}px`);
            })
            .on("mouseout", () => tooltip.style("visibility", "hidden"));

        // Min tick with adjusted position
        svg.append("line")
            .attr("x1", matrixSize + axisLabelOffset + xScale(min[i]))
            .attr("x2", matrixSize + axisLabelOffset + xScale(min[i]))
            .attr("y1", y)
            .attr("y2", y + cellSize - 1)
            .style("stroke", "black")
            .on("mouseover", () => {
                tooltip.style("visibility", "visible")
                    .text(`${label} min: ${min[i].toFixed(2)}`);
            })
            .on("mousemove", event => {
                tooltip.style("top", `${event.pageY - 10}px`)
                    .style("left", `${event.pageX + 10}px`);
            })
            .on("mouseout", () => tooltip.style("visibility", "hidden"));

        // Max tick with adjusted position
        svg.append("line")
            .attr("x1", matrixSize + axisLabelOffset + xScale(max[i]))
            .attr("x2", matrixSize + axisLabelOffset + xScale(max[i]))
            .attr("y1", y)
            .attr("y2", y + cellSize - 1)
            .style("stroke", "black")
            .on("mouseover", () => {
                tooltip.style("visibility", "visible")
                    .text(`${label} max: ${max[i].toFixed(2)}`);
            })
            .on("mousemove", event => {
                tooltip.style("top", `${event.pageY - 10}px`)
                    .style("left", `${event.pageX + 10}px`);
            })
            .on("mouseout", () => tooltip.style("visibility", "hidden"));
    });
}
