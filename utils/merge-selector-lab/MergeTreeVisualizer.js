import { valueToColor, formatBytesWithUnit, determineTickStep } from './visualizeHelpers.js';
import { infoButton } from './infoButton.js';

class MergeTreeVisualizer {
    getMargin() { return { left: 50, right: 40, top: 60, bottom: 60 }; }

    getLeft(part) { return part.left_bytes; }
    getRight(part) { return part.right_bytes; }

    // Override these to adjust values only for drawing merges
    getMergeLeft(part) { return this.xScale(this.getLeft(part)); }
    getMergeRight(part) { return this.xScale(this.getRight(part)); }
    getMergeTop(part) { return this.yScale(this.getTop(part)); }
    getMergeBottom(part) { return this.yScale(this.getBottom(part)); }

    // Override these to adjust values only for drawing parts
    getPartLeft(part) { return this.xScale(this.getLeft(part)); }
    getPartRight(part) { return this.xScale(this.getRight(part)); }
    getPartTop(part) { return this.yScale(this.getBottom(part)) + (this.isYAxisReversed() ? 0 : -this.part_height); }
    getPartBottom(part) { return this.yScale(this.getBottom(part)) + (this.isYAxisReversed() ? this.part_height : 0); }

    // Colors
    getMergeColor() { return "red"; }
    getPartColor() { return "black"; }
    getPartMarkColor() { return "yellow"; }

    isYAxisReversed() { return false; }

    constructor(mt, container) {
        // Cleanup previous visualization
        const oldSvg = container.select("svg");
        if (oldSvg.node()) {
            if (oldSvg.node().__tippy)
                oldSvg.node().__tippy.destroy();
        }
        oldSvg.remove();

        // Input visuals (common settings)
        this.margin = this.getMargin();
        this.width = 400;
        this.height = 450;
        this.part_height = 4;
        this.part_mark_width = 1;
        this.svgWidth = this.width + this.margin.left + this.margin.right;
        this.svgHeight = this.height;

        // Compute useful aggregates
        this.log_min_bytes = Math.log2(d3.min(mt.parts, d => d.bytes));
        this.log_max_bytes = Math.log2(d3.max(mt.parts, d => d.bytes));
        this.max_source_part_count = d3.max(mt.parts, d => d.source_part_count);

        // Compute scale ranges
        this.minXValue = d3.min(mt.parts, d => this.getLeft(d));
        this.maxXValue = d3.max(mt.parts, d => this.getRight(d));
        if (this.isYAxisReversed()) {
            this.minYValue = d3.min(mt.parts, d => this.getTop(d));
            this.maxYValue = d3.max(mt.parts, d => this.getBottom(d));
        } else {
            this.minYValue = d3.min(mt.parts, d => this.getBottom(d));
            this.maxYValue = d3.max(mt.parts, d => this.getTop(d));
        }

        // Create the SVG container
        this.svgContainer = container
            .append("svg")
            .attr("width", this.svgWidth)
            .attr("height", this.svgHeight);
    }

    initXScaleLinear() {
        // Set up the horizontal scale (x-axis) — linear scale
        this.xScale = d3.scaleLinear()
            .domain([this.minXValue, this.maxXValue])
            .range([this.margin.left, this.svgWidth - this.margin.right]);
    }

    getYRange() {
        const range = [this.svgHeight - this.margin.bottom, this.margin.top];
        return this.isYAxisReversed() ? [range[1], range[0]] : range;
    }

    initYScalePowersOfTwo() {
        // Set up the vertical scale (y-axis) — logarithmic scale
        this.yScale = d3.scaleLog()
            .base(2)
            .domain([Math.max(1, this.minYValue), Math.pow(2, Math.ceil(Math.log2(this.maxYValue)))])
            .range(this.getYRange());
    }

    initYScaleLinear() {
        // Set up the vertical scale (y-axis) — linear scale
        this.yScale = d3.scaleLinear()
            .domain([this.minYValue, this.maxYValue])
            .range(this.getYRange());
    }

    createXAxisLinear() {
        this.xAxis = this.isYAxisReversed() ? d3.axisTop(this.xScale) : d3.axisBottom(this.xScale);

        const tickStep = determineTickStep(this.maxXValue);
        const translateY = this.isYAxisReversed() ? this.margin.top : this.svgHeight - this.margin.bottom;
        this.svgContainer.append("g")
            .attr("transform", `translate(0, ${translateY})`)
            .call(this.xAxis.tickValues(d3.range(0, this.maxXValue, tickStep)).tickFormat(formatBytesWithUnit));

        // Add axis title
        this.svgContainer.append("text")
            .attr("x", this.svgWidth / 2)
            .attr("y", this.isYAxisReversed() ? (this.margin.top / 2 - 5) : (this.svgHeight - this.margin.bottom / 2 + 20) )
            .attr("text-anchor", "middle")
            .attr("font-size", "14px")
            .text("Bytes");
    }

    createYAxisPowersOfTwo() {
        const powersOfTwo = Array.from({ length: 50 }, (v, i) => Math.pow(2, i + 1));
        this.yAxis = d3.axisLeft(this.yScale)
            .tickValues(powersOfTwo.filter(d => d >= this.minYValue && d <= this.maxYValue))
            .tickFormat(d => `2^${Math.log2(d)}`);
        const yAxisGroup = this.svgContainer.append("g")
            .attr("transform", `translate(${this.margin.left}, 0)`)
            .call(this.yAxis);

        // Add axis title
        this.svgContainer.append("text")
            .attr("transform", "rotate(-90)")
            .attr("x", -this.svgHeight / 2)
            .attr("y", this.margin.left / 2 - 10)
            .attr("text-anchor", "middle")
            .attr("font-size", "14px")
            .text("Log(PartSize)");

        yAxisGroup.selectAll(".tick text")
            .each(function(d) {
                const exponent = Math.log2(d);
                const self = d3.select(this);
                self.text("");
                self.append("tspan").text("2");
                self.append("tspan")
                    .attr("dy", "-0.7em")
                    .attr("font-size", "70%").text(exponent);
            });
    }

    createYAxisLinear() {
        this.yAxis = d3.axisLeft(this.yScale)
            .tickArguments([5])
            .tickFormat(d => Number.isInteger(d) ? d : "");
        this.svgContainer.append("g")
            .attr("transform", `translate(${this.margin.left}, 0)`)
            .call(this.yAxis);

        // Add axis title
        this.svgContainer.append("text")
            .attr("transform", "rotate(-90)")
            .attr("x", -this.svgHeight / 2)
            .attr("y", this.margin.left / 2 - 5)
            .attr("text-anchor", "middle")
            .attr("font-size", "14px")
            .text("Source parts count");
    }

    createDescription(text, x = 10, y = 60) {
        this.svgContainer.node().__tippy = infoButton(this.svgContainer, x, y, text);
    }

    pxl(value, min = 0) { return Math.max(min, Math.floor(value)); }
    pxr(value, min = 1) { return Math.max(min, Math.ceil(value)); }
    pxt(value) { return value; }
    pxb(value) { return Math.max(1, value); }

    createMerges(mt) {
        // Append rectangles for merges
        this.svgContainer.append("g").attr("class", "viz-merge").selectAll("rect")
            .data(mt.parts)
            .enter()
            .filter(d => !d.active)
            .append("rect")
            .attr("x", d => this.pxl(this.getMergeLeft(d)))
            .attr("y", d => this.pxt(this.getMergeTop(d)))
            .attr("width", d => this.pxr(this.getMergeRight(d) - this.getMergeLeft(d)))
            .attr("height", d => this.pxb(this.getMergeBottom(d) - this.getMergeTop(d)))
            .attr("fill", d => this.getMergeColor(d));
    }

    createParts(mt) {
        // Append rectangles for parts
        this.svgContainer.append("g").attr("class", "viz-part").selectAll("rect")
            .data(mt.parts)
            .enter()
            .append("rect")
            .attr("x", d => this.pxl(this.getPartLeft(d)))
            .attr("y", d => this.pxt(this.getPartTop(d)))
            .attr("width", d => this.pxr(this.getPartRight(d) - this.getPartLeft(d)))
            .attr("height", d => this.pxb(this.getPartBottom(d) - this.getPartTop(d)))
            .attr("fill", d => this.getPartColor(d));
    }

    createPartMarks(mt) {
        // Append marks for parts begin
        this.svgContainer.append("g").attr("class", "viz-part-mark").selectAll("rect")
            .data(mt.parts)
            .enter()
            .append("rect")
            .attr("x", d => this.pxl(this.getPartLeft(d)))
            .attr("y", d => this.pxt(this.getPartTop(d)))
            .attr("width", d => this.pxr(Math.min(this.part_mark_width, this.getPartRight(d) - this.getPartLeft(d))))
            .attr("height", d => this.pxb(this.getPartBottom(d) - this.getPartTop(d)))
            .attr("fill", d => this.getPartMarkColor(d));
    }
}

class MergeTreeUtilityVisualizer extends MergeTreeVisualizer {
    getMargin() { return { left: 50, right: 30, top: 60, bottom: 60 }; }

    getTop(part) { return part.active ? part.bytes : part.parent_part.bytes; }
    getBottom(part) { return part.bytes; }

    getMergeColor(part) {
        // It is a constant to get consistent colors on all diagrams, merging more than 128 parts is not common
        const max_entropy = 7;
        return valueToColor(part.parent_part.entropy, 0, max_entropy);
    }

    getPartColor(part) {
        return part.merging ? "grey" : "black";
    }

    getPartMarkColor(part) {
        return "yellow";
    }

    constructor(mt, container) {
        super(mt, container);

        this.initXScaleLinear();
        this.initYScalePowersOfTwo();

        this.createMerges(mt);
        this.createParts(mt);
        this.createPartMarks(mt);

        this.createXAxisLinear();
        this.createYAxisPowersOfTwo();

        this.createDescription(`
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
}

class MergeTreeTimeVisualizer extends MergeTreeVisualizer {
    getMargin() { return { left: 50, right: 40, top: 60, bottom: 60 }; }

    isYAxisReversed() { return true; }
    getTop(part) { return part.active ? 0 : this.getBottom(part.parent_part); }
    getBottom(part) { return part.active ? 0 : part.parent_part.source_part_count + this.getBottom(part.parent_part); }

    getMergeTop(part) {  return this.yScale(this.getTop(part)); }
    getMergeBottom(part) { return this.yScale(this.getBottom(part)); }

    getMergeColor(part) {
        // TODO: choose consistent color scheme.
        // TODO: There is no point in showing height again with color
        return valueToColor(part.parent_part.source_part_count, 2, this.max_source_part_count, 90, 70, 270, 180);
    }

    getPartColor(part) {
        return part.merging ? "orange" : "yellow";
    }

    getPartMarkColor(part) {
        return "black";
    }

    constructor(mt, container) {
        super(mt, container);

        this.initXScaleLinear();
        this.initYScaleLinear();

        this.createMerges(mt);
        this.createParts(mt);
        this.createPartMarks(mt);

        this.createXAxisLinear();
        this.createYAxisLinear();

        this.createDescription(`
            <h5 align="center">Time diagram</h5>

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
        `, 15, 60);
    }
}

export { MergeTreeUtilityVisualizer, MergeTreeTimeVisualizer };
