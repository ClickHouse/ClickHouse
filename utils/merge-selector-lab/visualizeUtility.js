import { valueToColor, formatBytesWithUnit, determineTickStep } from './visualizeHelpers.js';

export function visualizeUtility(sim, container)
{
    {
        // Cleanup previous visualization
        const oldSvg = container.select("svg");
        if (oldSvg.node())
            oldSvg.node().__tippy.destroy();
        oldSvg.remove();
    }

    // Input visuals
    const margin = { left: 50, right: 30, top: 60, bottom: 60 };
    const width = 450;
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
        .filter(d => d.top !== undefined)
        .append("rect")
        .attr("x", d => pxl(xScale(d.left)))
        .attr("y", d => pxt(yScale(d.top)))
        .attr("width", d => pxr(xScale(d.left + d.bytes) - xScale(d.left)))
        .attr("height", d => pxb(yScale(d.bottom) - yScale(d.top)))
        .attr("fill", d => d.color);

    // Append rectangles for parts
    svgContainer.append("g").selectAll("rect")
        .data(mt)
        .enter()
        .append("rect")
        .attr("x", d => pxl(xScale(d.left)))
        .attr("y", d => pxt(yScale(d.bottom) - part_dy))
        .attr("width", d => pxr(xScale(d.left + d.bytes) - xScale(d.left)))
        .attr("height", d => pxb(part_dy))
        .attr("fill", "black");

    // Append marks for parts begin
    svgContainer.append("g").selectAll("rect")
        .data(mt)
        .enter()
        .append("rect")
        .attr("x", d => pxl(xScale(d.left)))
        .attr("y", d => pxt(yScale(d.bottom) - part_dy))
        .attr("width", d => pxr(Math.min(part_dx, xScale(d.left + d.bytes) - xScale(d.left))))
        .attr("height", d => pxb(part_dy))
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
        .tickFormat(d => `2^${Math.log2(d)}`);
    const yAxisGroup = svgContainer.append("g")
        .attr("transform", `translate(${margin.left}, 0)`) // Align with the left margin
        .call(yAxis);

    // Add label for the y-axis
    svgContainer.append("text")
        .attr("transform", "rotate(-90)")
        .attr("x", -svgHeight / 2)
        .attr("y", margin.left / 2 - 10)
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

    // Add description with an information circle icon using tippy.js for tooltips
    const infoGroup = svgContainer.append("g")
        .attr("class", "chart-description")
        .attr("transform", `translate(10, 60)`);

    infoGroup.append("text")
        .attr("text-anchor", "middle")
        .attr("y", 4)
        .attr("fill", "white")
        .style("font-size", "16px")
        .text("ℹ");

    // Initialize tippy.js tooltip for description
    svgContainer.node().__tippy = tippy(infoGroup.node(), {
        content: `
            This visualization represents a resulting merge tree after simulation.
            Evert <b>part</b> is represented by a black bar with yellow mark on left side.
            Parts are positioned vertically according to logarithm of their size.
            Every <b>merge</b> is represented by a number of rectangles that connect source parts at bottom to the resulting part at the top.
            Horizontal axis represent amount of inserted bytes: newly inserted parts appear on the right side.
            Width of a part represent its size.
            <u>Area of a merge represents its utility.</u>
            Total area does <i>not</i> depend on merge tree structure, it is a function of initial and final part sizes.
            Note that time is not present explicitly on this chart, but merges push data upwards and inserts expand chart to the right.
            <b>Color</b> of a merge-related rectangles might represent on of selected metric: resulting size, order or time of merge, number of source parts, etc.
        `,
        allowHTML: true,
        placement: 'right',
        theme: 'light',
        arrow: true,
    });
}
