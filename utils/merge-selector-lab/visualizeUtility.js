import { valueToColor, formatBytesWithUnit, determineTickStep } from './visualizeHelpers.js';
import { infoButton } from './infoButton.js';

export function visualizeUtility(mt, container)
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
    const width = 300;
    const height = 450;
    const part_dy = 4;
    const part_dx = 1;
    const max_entropy = 7; // It is a constant to get consistent colors on all diagrams, merging more than 128 parts is not common

    // Compute useful aggregates
    let log_min_bytes = Math.log2(d3.min(mt.parts, d => d.bytes));
    let log_max_bytes = Math.log2(d3.max(mt.parts, d => d.bytes));
    let max_source_part_count = d3.max(mt.parts, d => d.source_part_count);

    // Data
    let data = [];
    let lefts = {}; // Maps inserted part.begin to its part.left
    let insert_left = 0;
    for (const p of mt.parts)
    {
        const parent = p.parent == undefined ? null : mt.parts[p.parent];
        //console.log("ENTROPY", parent.entropy);
        data.push({
            bytes: p.bytes,
            created: p.created,
            left: p.level == 0? insert_left : lefts[p.begin],
            top: p.parent == undefined ? p.bytes : mt.parts[p.parent].bytes,
            bottom: p.bytes,
            /* entropy */ color: parent ? valueToColor(parent.entropy, 0, max_entropy) : undefined,
            // /* height */ color: parent ? undefined : valueToColor(Math.log2(parent.bytes), log_min_bytes, log_max_bytes),
            // /* order */ color: parent ? undefined : valueToColor(parent.idx, 0, mt.parts.length),
            // /* source_part_count */ color: parent ? valueToColor(parent.source_part_count, 2, max_source_part_count) : undefined,
            part: p
        });
        if (p.level == 0)
        {
            lefts[p.begin] = insert_left;
            insert_left += p.bytes;
        }
    }

    const minYValue = d3.min(data, d => d.bottom);
    const maxYValue = Math.max(minYValue * 2, d3.max(data, d => d.top));
    const maxXValue = d3.max(data, d => d.bytes + d.left);

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
        .data(data)
        .enter()
        .filter(d => d.part.parent !== undefined)
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
        .attr("y", d => pxt(yScale(d.bottom) - part_dy))
        .attr("width", d => pxr(xScale(d.left + d.bytes) - xScale(d.left)))
        .attr("height", d => pxb(part_dy))
        .attr("fill", d => d.part.merging ? "grey" : "black");

    // Append marks for parts begin
    svgContainer.append("g").selectAll("rect")
        .data(data)
        .enter()
        .append("rect")
        .attr("x", d => pxl(xScale(d.left)))
        .attr("y", d => pxt(yScale(d.bottom) - part_dy))
        .attr("width", d => pxr(Math.min(part_dx, xScale(d.left + d.bytes) - xScale(d.left))))
        .attr("height", d => pxb(part_dy))
        .attr("fill", d => d.part.merging ? "orange" : "yellow");

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
    svgContainer.node().__tippy = infoButton(svgContainer, 10, 60, `
        <h5 align="center">Utility diagram</h5>

        <h6>Parts</h6>
        <p>
        The diagram is constructed from the bottom upward.
        Initial parts are at the bottom.
        Part is a horizontal black bar with a yellow mark on the left side.
        Bar width equals part size.
        Parts are positioned on the X-axis in the insertion order: older part are on the left, newer are on the right.
        The y-position of a part equals the logarithm of its size.
        </p>

        <h6>Merges</h6>
        <p>
        One merge is represented by shape that consists of adjacent rectangles of the same color.
        One rectangle per every child (source part).
        Every rectangle connects the child at the bottom to the parent (resulting) part at the top.
        The area of a merge represents its <u>utility</u>.
        Color represents average height of merge, <u>entropy</u>: its area divided by its width.
        </p>

        <h6>Utility</h6>
        <p>
        Note that total diagram area does not depend on merge tree structure: it is a function of initial and final part sizes.
        So larger utility of a merge means "more" progress toward the "all parts are merged" state.
        The utility is a measure that shows the overall "progress" of the merging process,
        a total "distance" that all byte of data should travel upwards to be merged:

        $$U = B \\log B - \\sum_{i} b_{i} \\log b_{i}$$

        where $B$ – size of result, $b_{i}$ – size of $i$-th child.
        </p>

        <h6>Entropy</h6>
        <p>
        Consider a vertical line on the diagram.
        It show a path of one single byte from the initial bottom part to the final top part.
        Number of times this line intersects black bars is equal to the write amplification.
        To lower write amplification it is important to have larger distance between bars.
        Entropy of a merge is equal to average (per byte) distance between bars:

        $$H = \\frac{U}{B} = - \\sum_{i} p_{i} \\log p_{i}$$
        where $p_{i}$ – probability to randomly select a byte from $i$-th child.
        </p>
    `);
}
