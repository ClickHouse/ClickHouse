import { renderMath } from './renderMath.js';
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
            is_leftmost: parent ? (parent.begin == child.begin ? 1 : 0): 0,
            is_rightmost: parent ? (parent.end == child.end ? 1 : 0): 0,
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
        .attr("x", d => pxl(xScale(d.left) + d.is_leftmost))
        .attr("y", d => pxt(yScale(d.top)))
        .attr("width", d => pxr(xScale(d.left + d.bytes) - xScale(d.left) - d.is_leftmost))
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
        .text("Bytes");

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
        content: renderMath(`
            <h5 align="center">Execution time diagram</h5>

            <h6>Parts</h6>
            <p>
            The diagram is constructed from the top downwards.
            Final parts are at the top.
            Part is a horizontal yellow bar with a black mark on the left side.
            Bar width equals part size.
            Parts are positioned on the X-axis in the insertion order: older part are on the left, newer are on the right.
            The y-position of a part depends of history of merges (see below).
            </p>

            <h6>Merges</h6>
            <p>
            One merge is represented by one rectangle.
            It connects the children at the bottom to the parent (resulting) part at the top.
            The height of the rectangle equals the number of source parts.
            It determines the y-position of all parts relative to their parents.
            Color also represents the number of source parts in the merge.
            </p>

            <h6>Execution time</h6>
            <p>
            The X-axis is in bytes.
            In model merge duration is proportional to number of bytes to be written.
            So width of merges also represents duration (execution time) of a merge.
            Such a model assumes there is no overhead for merges and speed of all merges expressed in bytes written per second is the same.
            </p>

            <h6>Part count time integral</h6>
            <p>
            Average active part count is computed as an integral of part count over time interval:
            $$I = T \\cdot \\mathbb{E}[Active] = \\int_0^T Active(t) \\, dt$$
            To minimize part count one should minimize the integral.
            It can be computed as total area of all rectangles in the diagram plus "waiting" time $W$.
            The area of one merge rectangle equals its contribution to the integral.
            $$I = W + \\sum_{i} d_{i} \\cdot n_{i}$$
            where $d_{i}$ – merge execution time of $i$-th part, $n_{i}$ – number of children of $i$-th part.
            </p>
            <p>
            It is important to note that diagram does not show "waiting" time, while parts are active, but not merging.
            </p>
        `),
        allowHTML: true,
        placement: 'bottom',
        theme: 'light',
        trigger: 'click',
        arrow: true,
        maxWidth: '600px'
    });
}
