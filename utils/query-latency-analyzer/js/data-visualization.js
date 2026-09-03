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
    const cellSize = 10;
    const axisLabelOffset = 260;
    const matrixSize = cellSize * n;
    const bandMargin = 20;
    // Use the pre-computed available width, subtract the matrix and fixed elements
    const bandAreaWidth = (window.availableWidth || 1200) - matrixSize - axisLabelOffset - bandMargin - 270;
    const margin = {
        top: 2,
        right: axisLabelOffset + bandAreaWidth + bandMargin,
        bottom: 18,
        left: 20
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

    // Compute share for each label
    const labelShares = new Array(n).fill(0);
    for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
            if (!labels[i].startsWith("_") && !labels[j].startsWith("_")) {
                labelShares[i] += cov[i][j];
            }
        }
        labelShares[i] = labelShares[i] / total;
    }

    const norm = cov.map(row => row.map(v => v / total));
    for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
            if (i < j) {
                norm[i][j] += norm[j][i];
                norm[j][i] = null;
            }
        }
    }

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
        .each(function(d) {
            tippy(this, {
                content: `${labels[d.i]} × ${labels[d.j]}: ${(d.v * 100).toFixed(2)}%`,
                placement: 'left',
                theme: 'latency-data',
                delay: [0, 0],
                duration: [0, 0]
            });
        });

    // Add share percentages as text elements
    svg.selectAll(".share-text")
        .data(labelShares.map((share, i) => ({
            share,
            label: labels[i],
            y: labelToY.get(labels[i]),
            x: i * cellSize // Use same x-coordinate as diagonal cells
        })))
        .enter()
        .filter(d => !d.label.startsWith('_'))
        .append("text")
        .attr("class", "share-text")
        .attr("x", d => d.x - 2)
        .attr("y", d => d.y + cellSize - 2) // Align with the diagonal cells
        .attr("text-anchor", "end")
        .style("font-size", "8px")
        .style("font-weight", "bold")
        .text(d => (d.share * 100).toFixed(0) + '%');

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

    // Distribution bands with consistent scale across all visualizations
    const xScale = d3.scaleLinear()
        .domain([0, window.globalMaxValue || d3.max(max)])
        .range([0, bandAreaWidth]);

    // Add ruler elements
    const ruler = svg.append("line")
        .attr("class", "ruler")
        .attr("y1", 0)
        .attr("y2", totalHeight)
        .style("stroke", "black")
        .style("stroke-dasharray", "2,2")
        .style("opacity", 0);

    // Create label as HTML element for better positioning
    const rulerLabel = container.append("div")
        .style("position", "absolute")
        .style("background", "white")
        .style("padding", "0 4px")
        .style("font-size", "10px")
        .style("pointer-events", "none")
        .style("opacity", 0)
        .style("transform", "translate(-50%, 0)")  // Center horizontally only

    // Create invisible overlay for mouse tracking
    const overlay = svg.append("rect")
        .attr("x", matrixSize + axisLabelOffset)
        .attr("y", 0)
        .attr("width", bandAreaWidth)
        .attr("height", totalHeight)
        .style("fill", "none")
        .style("pointer-events", "all")
        .on("mousemove", (event) => {
            const [x] = d3.pointer(event);
            const adjustedX = x - (matrixSize + axisLabelOffset);
            if (adjustedX >= 0 && adjustedX <= bandAreaWidth) {
                const xPos = matrixSize + axisLabelOffset + adjustedX;
                const value = xScale.invert(adjustedX);
                ruler
                    .attr("x1", xPos)
                    .attr("x2", xPos)
                    .style("opacity", 1);

                // Format value using K/M/G/T
                let formattedValue;
                if (Math.abs(value) >= 1e12) formattedValue = (value / 1e12).toFixed(1) + 'T';
                else if (Math.abs(value) >= 1e9) formattedValue = (value / 1e9).toFixed(1) + 'G';
                else if (Math.abs(value) >= 1e6) formattedValue = (value / 1e6).toFixed(1) + 'M';
                else if (Math.abs(value) >= 1e3) formattedValue = (value / 1e3).toFixed(1) + 'K';
                else formattedValue = value.toFixed(1);

                // Get actual position relative to page for the label
                const svgRect = container.select("svg").node().getBoundingClientRect();
                rulerLabel
                    .style("left", (svgRect.left + xPos) + "px")
                    .style("top", (svgRect.top + totalHeight + margin.top - 7) + "px")  // Adjusted to align with x-axis
                    .text(formattedValue)
                    .style("opacity", 1);
            }
        })
        .on("mouseleave", () => {
            ruler.style("opacity", 0);
            rulerLabel.style("opacity", 0);
        });

    labels.forEach((label, i) => {
        const y = labelToY.get(label);
        const percMap = p[i];
        const percentileKeys = Object.keys(percMap).map(Number).sort((a, b) => a - b);

        percentileKeys.forEach((perc, idx) => {
            const val = percMap[perc];
            const nextVal = idx < percentileKeys.length - 1 ? percMap[percentileKeys[idx + 1]] : max[i];
            const x0 = matrixSize + axisLabelOffset + xScale(val);
            const x1 = matrixSize + axisLabelOffset + xScale(nextVal);
            const rect = svg.append("rect")
                .attr("x", Math.floor(x0))
                .attr("y", y)
                .attr("width", Math.ceil(x1 - x0))
                .attr("height", cellSize - 1)
                .style("fill", colorScale(perc / 100))
                .node();

            tippy(rect, {
                content: `${label} ${perc}th percentile: ${val.toFixed(2)}`,
                placement: 'left',
                theme: 'latency-data',
                delay: [0, 0],
                duration: [0, 0]
            });
        });

        // Standard deviation bar
        const stdLeft = avg[i] - std[i];
        const stdRight = avg[i] + std[i];
        const stdBar = svg.append("line")
            .attr("x1", matrixSize + axisLabelOffset + xScale(Math.max(0, stdLeft)))
            .attr("x2", matrixSize + axisLabelOffset + xScale(stdRight))
            .attr("y1", y + cellSize / 2)
            .attr("y2", y + cellSize / 2)
            .style("stroke", "black")
            .node();

        tippy(stdBar, {
            content: `${label} ±1σ: [${stdLeft.toFixed(2)}, ${stdRight.toFixed(2)}]`,
            placement: 'left',
            theme: 'latency-data',
            delay: [0, 0],
            duration: [0, 0]
        });

        // Mean point
        const meanCircle = svg.append("circle")
            .attr("cx", matrixSize + axisLabelOffset + xScale(avg[i]))
            .attr("cy", y + cellSize / 2)
            .attr("r", 2)
            .style("fill", "black")
            .node();

        tippy(meanCircle, {
            content: `${label} mean: ${avg[i].toFixed(2)}`,
            placement: 'left',
            theme: 'latency-data',
            delay: [0, 0],
            duration: [0, 0]
        });

        // Min tick
        const minTick = svg.append("line")
            .attr("x1", matrixSize + axisLabelOffset + xScale(min[i]))
            .attr("x2", matrixSize + axisLabelOffset + xScale(min[i]))
            .attr("y1", y)
            .attr("y2", y + cellSize - 1)
            .style("stroke", "black")
            .node();

        tippy(minTick, {
            content: `${label} min: ${min[i].toFixed(2)}`,
            placement: 'left',
            theme: 'latency-data',
            delay: [0, 0],
            duration: [0, 0]
        });

        // Max tick
        const maxTick = svg.append("line")
            .attr("x1", matrixSize + axisLabelOffset + xScale(max[i]))
            .attr("x2", matrixSize + axisLabelOffset + xScale(max[i]))
            .attr("y1", y)
            .attr("y2", y + cellSize - 1)
            .style("stroke", "black")
            .node();

        tippy(maxTick, {
            content: `${label} max: ${max[i].toFixed(2)}`,
            placement: 'left',
            theme: 'latency-data',
            delay: [0, 0],
            duration: [0, 0]
        });
    });

    // Add x-axis for distribution bands
    const xAxis = d3.axisBottom(xScale)
        .ticks(5)
        .tickFormat(d => {
            if (Math.abs(d) >= 1e12) return (d / 1e12).toFixed(1) + 'T';
            if (Math.abs(d) >= 1e9) return (d / 1e9).toFixed(1) + 'G';
            if (Math.abs(d) >= 1e6) return (d / 1e6).toFixed(1) + 'M';
            if (Math.abs(d) >= 1e3) return (d / 1e3).toFixed(1) + 'K';
            return d.toFixed(1);
        });

    svg.append("g")
        .attr("class", "axis")
        .attr("transform", `translate(${matrixSize + axisLabelOffset}, ${totalHeight})`)
        .call(xAxis);
}
