import { valueToColor, formatBytesWithUnit, determineTickStep } from './visualizeHelpers.js';

export function visualizeExecutionTime(sim, container)
{
    {
        // Cleanup previous visualization
        const oldSvg = container.select("svg");
        if (oldSvg.node())
            oldSvg.node().__tippy.destroy();
        oldSvg.remove();
    }

    // Input visuals
    const margin = { left: 50, right: 40, top: 60, bottom: 60 };
    const width = 450;
    const height = 450;
    const part_dy = 4;
    const part_dx = 1;

    // Compute useful aggregates
    let log_min_bytes = Math.log2(d3.min(sim.parts, d => d.bytes));
    let log_max_bytes = Math.log2(d3.max(sim.parts, d => d.bytes));
    let max_source_part_count = d3.max(sim.parts, d => d.source_part_count);

    // Prepare
    let lefts = {}; // Maps inserted part.begin to its part.left
    let insert_left = 0;
    for (const p of sim.parts)
    {
        if (p.level == 0)
        {
            lefts[p.begin] = insert_left;
            insert_left += p.bytes;
        }
    }

    // Construct data
    let data = [];
    for (let i = sim.parts.length - 1; i >= 0; i--)
    {
        const child = sim.parts[i];
        const parent = child.parent == undefined ? null : sim.parts[child.parent];
        const top = parent ? data[child.parent].bottom : 0;
        const bottom = parent ? top + parent.source_part_count : 0;
        data[i] = {
            bytes: child.bytes,
            created: child.created,
            left: lefts[child.begin],
            top,
            bottom,
            // /* height */ color: parent ? undefined : valueToColor(Math.log2(parent.bytes), log_min_bytes, log_max_bytes),
            // /* order */ color: parent ? undefined : valueToColor(parent.idx, 0, sim.parts.length),
            /* source_part_count */ color: parent ? valueToColor(parent.source_part_count, 2, max_source_part_count, 90, 70, 270, 180) : undefined,
            part: child
        };
    }

    const maxYValue = d3.max(data, d => d.bottom);
    const minYValue = d3.min(data, d => d.top);
    const maxXValue = d3.max(data, d => d.bytes + d.left);

    const svgWidth = width + margin.left + margin.right;
    const svgHeight = height;

    // Set up the horizontal scale (x-axis) — linear scale
    const xScale = d3.scaleLinear()
        .domain([0, maxXValue]) // Adjust the scale to account for total 'bytes'
        .range([margin.left, svgWidth - margin.right]); // Apply left and right margins

    // Set up the vertical scale (y-axis) — logarithmic scale
    const yScale = d3.scaleLinear()
        .domain([0, maxYValue])
        .range([margin.top, svgHeight - margin.bottom]);

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
        .data(data)
        .enter()
        .filter(d => d.bottom > 0)
        .append("rect")
        .attr("x", d => pxl(xScale(d.left)))
        .attr("y", d => pxt(yScale(d.top)))
        .attr("width", d => pxr(xScale(d.left + d.bytes) - xScale(d.left)))
        .attr("height", d => pxb(yScale(d.bottom) - yScale(d.top)))
        .attr("fill", d => d.color);

    // Append rectangles for parts
    svgContainer.append("g").selectAll("rect")
        .data(data)
        .enter()
        .append("rect")
        .attr("x", d => pxl(xScale(d.left)))
        .attr("y", d => pxt(yScale(d.bottom)))
        .attr("width", d => pxr(xScale(d.left + d.bytes) - xScale(d.left)))
        .attr("height", d => pxb(part_dy))
        .attr("fill", "yellow");

    // Append marks for parts begin
    svgContainer.append("g").selectAll("rect")
        .data(data)
        .enter()
        .append("rect")
        .attr("x", d => pxl(xScale(d.left)))
        .attr("y", d => pxt(yScale(d.bottom)))
        .attr("width", d => pxr(Math.min(part_dx, xScale(d.left + d.bytes) - xScale(d.left))))
        .attr("height", d => pxb(part_dy))
        .attr("fill", "black");

    // Determine the tick step based on maxValue
    const tickStep = determineTickStep(maxXValue);

    // Add the x-axis at the bottom of the SVG
    const xAxis = d3.axisTop(xScale)
        //.ticks(Math.ceil(maxXValue / tickStep)) // Dynamically set the number of ticks
        .tickValues(d3.range(0, maxXValue, tickStep)) // Generate ticks based on the calculated tick step
        .tickFormat(formatBytesWithUnit);
    svgContainer.append("g")
        .attr("transform", `translate(0, ${margin.top})`) // Position at the top
        .call(xAxis);

    // Add label for the x-axis
    svgContainer.append("text")
        .attr("x", svgWidth / 2)
        .attr("y", margin.top / 2 - 5)
        .attr("text-anchor", "middle")
        .attr("font-size", "14px")
        .text("Merge duration");

    // Add the y-axis
    const yAxis = d3.axisLeft(yScale)
        //.ticks(maxYValue)
        .tickArguments([5])
        .tickFormat(d => Number.isInteger(d) ? d : ""); // Only show integers
    svgContainer.append("g")
        .attr("transform", `translate(${margin.left}, 0)`) // Align with the left margin
        .call(yAxis);

    // Add label for the y-axis
    svgContainer.append("text")
        .attr("transform", "rotate(-90)")
        .attr("x", -svgHeight / 2)
        .attr("y", margin.left / 2 - 5)
        .attr("text-anchor", "middle")
        .attr("font-size", "14px")
        .text("Source parts count");

    // Add description with an information circle icon using tippy.js for tooltips
    const infoGroup = svgContainer.append("g")
        .attr("class", "chart-description")
        .attr("transform", `translate(15, 60)`);

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
            Every <b>part</b> is represented by yellow bar with black mark on the left side.
            Every <b>merge</b> is represented by one rectangle that connects source parts at bottom to the resulting part at the top.
            Height of merge rectangle equals number of source parts, width is merge duration (proportional to size in bytes).
            <u>Total area equals time integral of part count over time</u> that we would like to minimize.
            Note that this area do NOT show "waiting" time, while parts are not merging.
            <b>Color</b> of a merge-related rectangles might represent on of selected metric: resulting size, order or time of merge, number of source parts, etc.
        `,
        allowHTML: true,
        placement: 'right',
        theme: 'light',
        arrow: true,
    });
}
