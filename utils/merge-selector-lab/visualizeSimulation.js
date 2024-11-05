////////////////////////////////////////////////////////////////////////////////
// Helpers
//
function valueToColor(value, min, max)
{
    const hueMin = 120; // Start with
    const hueMax = 360; // Finish with
    const saturation = 70; // Constant saturation (same as steelblue)
    const lightness = 50;  // Constant lightness (same as steelblue)

    // Ensure value is within the min/max range
    if (value < min) value = min;
    if (value > max) value = max;


    // Normalize the value to range from 0 to 1
    const normalizedValue = (value - min) / (max - min);

    // Calculate hue based on the normalized value
    const hue = hueMin + normalizedValue * (hueMax - hueMin);

    // Return the HSL color string
    return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
}

// Custom tick format function for displaying bytes with dynamic scaling (KB, MB, GB)
function formatBytesWithUnit(bytes)
{
    if (bytes >= Math.pow(1024, 3))
        return (bytes / Math.pow(1024, 3)).toFixed(1) + ' GB';
    else if (bytes >= Math.pow(1024, 2))
        return (bytes / Math.pow(1024, 2)).toFixed(1) + ' MB';
    else if (bytes >= 1024)
        return (bytes / 1024).toFixed(1) + ' KB';
    else
        return bytes + ' B';
}

// Dynamically determine appropriate tick step and unit based on the max value
function determineTickStep(maxValue)
{
    let step;
    let unit;

    if (maxValue >= Math.pow(1024, 3)) // For GB range
        unit = Math.pow(1024, 3); // 1 GB
    else if (maxValue >= Math.pow(1024, 2)) // For MB range
        unit = Math.pow(1024, 2); // 1 MB
    else if (maxValue >= 1024) // For KB range
        unit = 1024; // 1 KB
    else
        unit = 1; // Bytes range

    // Determine appropriate tick step based on the range (1, 2, 5, 10, 20, 50, 100, 200...)
    if (maxValue / unit > 500)
        step = 100 * unit;
    else if (maxValue / unit > 200)
        step = 50 * unit;
    else if (maxValue / unit > 100)
        step = 20 * unit;
    else if (maxValue / unit > 50)
        step = 10 * unit;
    else if (maxValue / unit > 20)
        step = 5 * unit;
    else if (maxValue / unit > 10)
        step = 2 * unit;
    else
        step = 1 * unit;

    return step;
}

////////////////////////////////////////////////////////////////////////////////
// Visualize with D3 and SVG
//
export function visualizeSimulation(sim, container)
{
    // Input visuals
    const margin = { left: 60, right: 40, top: 40, bottom: 60 };
    const width = 930;
    const height = 450;
    const part_dy = 4;
    const part_dx = 1;

    // Compute useful aggregates
    let log_min_bytes = Math.log2(d3.min(sim.parts, d => d.bytes));
    let log_max_bytes = Math.log2(d3.max(sim.parts, d => d.bytes));
    let max_source_part_count = d3.max(sim.parts, d => d.source_part_count);

    // Data
    let mt = [];
    let lefts = {}; // Maps inserted part.begin to its part.left
    let insert_left = 0;
    for (const p of sim.parts)
    {
        mt.push({
            bytes: p.bytes,
            created: p.created,
            left: p.level == 0? insert_left : lefts[p.begin],
            top: p.parent == undefined ? undefined : sim.parts[p.parent].bytes,
            bottom: p.bytes,
            /* height */ color: p.parent == undefined ? undefined : valueToColor(Math.log2(sim.parts[p.parent].bytes), log_min_bytes, log_max_bytes),
            // /* order */ color: p.parent == undefined ? undefined : valueToColor(sim.parts[p.parent].idx, 0, sim.parts.length),
            // /* source_part_count */ color: p.parent == undefined ? undefined : valueToColor(sim.parts[p.parent].source_part_count, 2, max_source_part_count),
            part: p
        });
        if (p.level == 0)
        {
            lefts[p.begin] = insert_left;
            insert_left += p.bytes;
        }
    }

    const maxYValue = d3.max(mt, d => d.top);
    const minYValue = d3.min(mt, d => d.bottom);
    const maxXValue = d3.max(mt, d => d.bytes + d.left);

    const svgWidth = width + margin.left + margin.right;
    const svgHeight = height;

    // Set up the horizontal scale (x-axis) — linear scale
    const xScale = d3.scaleLinear()
        .domain([0, maxXValue]) // Adjust the scale to account for total 'bytes'
        .range([margin.left, svgWidth - margin.right]); // Apply left and right margins

    // Set up the vertical scale (y-axis) — logarithmic scale
    const yScale = d3.scaleLog()
        .base(2) // Use base 2 for the logarithmic scale
        .domain([Math.max(1, minYValue), Math.pow(2, Math.ceil(Math.log2(maxYValue)))]) // Fit the data into powers of 2
        .range([svgHeight - margin.bottom, margin.top]); // Page coordinates (bottom to top)

    // Create the SVG container
    const svgContainer = container
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);

    // To avoid negative width and height
    function pxl(value, min = 0) { return Math.max(min, Math.floor(value)); }
    function pxr(value, min = 1) { return Math.max(min, Math.ceil(value)); }
    // function pxt(value, min = 0) { return Math.max(min, Math.floor(value)); }
    // function pxb(value, min = 1) { return Math.max(min, Math.ceil(value)); }
    // function pxt(value) { return value; }
    // function pxb(value) { return Math.max(1, value); }
    function pxt(value) { return value; }
    function pxb(value) { return Math.max(1, value); }

    // Append rectangles for merges
    svgContainer.append("g").selectAll("rect")
        .data(mt)
        .enter()
        .filter(function(d) { return d.top !== undefined; })
        .append("rect")
        .attr("x", function(d) { return pxl(xScale(d.left)); })
        .attr("y", function(d) { return pxt(yScale(d.top)); })
        .attr("width", function(d) { return pxr(xScale(d.left + d.bytes) - xScale(d.left)); })
        .attr("height", function(d) { return pxb(yScale(d.bottom) - yScale(d.top)); })
        .attr("fill", function(d) {return d.color} );

    // Append rectangles for parts
    svgContainer.append("g").selectAll("rect")
        .data(mt)
        .enter()
        .append("rect")
        .attr("x", function(d) { return pxl(xScale(d.left)); })
        .attr("y", function(d) { return pxt(yScale(d.bottom) - part_dy); })
        .attr("width", function(d) { return pxr(xScale(d.left + d.bytes) - xScale(d.left)); })
        .attr("height", function(d) { return pxb(part_dy); })
        .attr("fill", "black");

    // Append marks for parts begin
    svgContainer.append("g").selectAll("rect")
        .data(mt)
        .enter()
        .append("rect")
        .attr("x", function(d) { return pxl(xScale(d.left)); })
        .attr("y", function(d) { return pxt(yScale(d.bottom) - part_dy); })
        .attr("width", function(d) { return pxr(Math.min(part_dx, xScale(d.left + d.bytes) - xScale(d.left))); })
        .attr("height", function(d) { return pxb(part_dy); })
        .attr("fill", "yellow");

    // Determine the tick step based on maxValue
    const tickStep = determineTickStep(maxXValue);

    // Add the x-axis at the bottom of the SVG
    const xAxis = d3.axisBottom(xScale)
        //.ticks(Math.ceil(maxXValue / tickStep)) // Dynamically set the number of ticks
        .tickValues(d3.range(0, maxXValue, tickStep)) // Generate ticks based on the calculated tick step
        .tickFormat(formatBytesWithUnit);
    svgContainer.append("g")
        .attr("transform", `translate(0, ${svgHeight - margin.bottom})`) // Position at the bottom
        .call(xAxis);

    // Add label for the x-axis
    svgContainer.append("text")
        .attr("x", svgWidth / 2)
        .attr("y", svgHeight - margin.bottom / 2 + 20)
        .attr("text-anchor", "middle")
        .attr("font-size", "14px")
        .text("Bytes");

    // Add the y-axis with custom ticks for powers of 2
    const powersOfTwo = Array.from({ length: 50 }, (v, i) => Math.pow(2, i + 1));
    const yAxis = d3.axisLeft(yScale)
        .tickValues(powersOfTwo.filter(d => d >= minYValue && d <= maxYValue))
        .tickFormat(function(d) { return `2^${Math.log2(d)}`; });
    const yAxisGroup = svgContainer.append("g")
        .attr("transform", `translate(${margin.left}, 0)`) // Align with the left margin
        .call(yAxis);

    // Add label for the y-axis
    svgContainer.append("text")
        .attr("transform", "rotate(-90)")
        .attr("x", -svgHeight / 2)
        .attr("y", margin.left / 2 - 15)
        .attr("text-anchor", "middle")
        .attr("font-size", "14px")
        .text("Log(PartSize)");

    // Modify the tick labels using <tspan> to position the exponent properly
    yAxisGroup.selectAll(".tick text")
        .each(function(d)
        {
            const exponent = Math.log2(d);  // Calculate the exponent
            const self = d3.select(this);

            // Create base '2'
            self.text("");  // Clear current text
            self.append("tspan").text("2");

            // Add the exponent in a superscript-like way using <tspan> and dy positioning
            self.append("tspan")
                .attr("dy", "-0.7em")  // Move the exponent up
                .attr("font-size", "70%")  // Reduce font size for the exponent
                .text(exponent);
        });

    // Append text with metrics
    svgContainer.append("text")
        .attr("x", svgWidth - 50) // Position near the right edge of the SVG
        .attr("y", 20) // Position near the top edge of the SVG
        .attr("text-anchor", "end") // Align the text to the right
        .attr("font-size", "14px") // Set font size
        .attr("fill", "black") // Set text color
        .text(`
            ${sim.title === undefined ? "" : sim.title}
            Total: ${sim.written_bytes / 1024 / 1024} MB,
            Inserted: ${sim.inserted_bytes / 1024 / 1024} MB,
            WA: ${sim.writeAmplification().toFixed(2)},
            AvgPartCount: ${sim.avgActivePartCount().toFixed(2)}
            Time: ${(sim.current_time).toFixed(2)}s
        `);
}
