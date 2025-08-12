import { infoButton } from './infoButton.js';

export class Chart {
    constructor(selection = d3.select("body"), xTitle = "", yTitle = "", description = "")
    {
        // Set up dimensions and margins
        this.width = document.documentElement.clientWidth;
        this.height = 400;
        this.margin = { top: 20, right: 30, bottom: 60, left: 50 };

        // Initialize empty series array
        this.colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]; // Deterministic colors

        this.min_track = false;

        this.selection = selection;
        this.xTitleText = xTitle;
        this.yTitleText = yTitle;
        this.descriptionText = description;
        this.initChart();
    }

    trackMin(value = true)
    {
        this.min_track = value;
        return this;
    }

    initChart()
    {
        // Clear
        this.minY = Infinity;
        this.series = [];

        // Create the SVG container
        this.rootSvg = this.selection
            .append("svg")
            .attr("width", this.width)
            .attr("height", this.height)
        this.svg = this.rootSvg
            .append("g")
            .attr("transform", `translate(${this.margin.left}, ${this.margin.top})`);

        this.innerWidth = this.width - this.margin.left - this.margin.right;
        this.innerHeight = this.height - this.margin.top - this.margin.bottom;

        // Set up scales
        this.xScale = d3.scaleLinear()
            .domain([0, 10]) // Start with an arbitrary domain
            .range([0, this.innerWidth]);

        this.yScale = d3.scaleLinear()
            .domain([0, 10]) // Start with an arbitrary domain
            .nice()
            .range([this.innerHeight, 0]);

        // Add x-axis
        this.svg.append("g")
            .attr("transform", `translate(0, ${this.innerHeight})`)
            .attr("class", "x-axis")
            .call(d3.axisBottom(this.xScale));

        // Add y-axis
        this.svg.append("g")
            .attr("class", "y-axis")
            .call(d3.axisLeft(this.yScale));

        // Add x-axis title
        this.svg.append("text")
            .attr("class", "x-axis-title")
            .attr("x", this.innerWidth / 2)
            .attr("y", this.innerHeight + this.margin.bottom - 10)
            .attr("text-anchor", "middle")
            .style("font-size", "14px")
            .text(this.xTitleText);

        // Add y-axis title
        this.svg.append("text")
            .attr("class", "y-axis-title")
            .attr("x", -(this.innerHeight / 2))
            .attr("y", -this.margin.left + 15)
            .attr("transform", "rotate(-90)")
            .attr("text-anchor", "middle")
            .style("font-size", "14px")
            .text(this.yTitleText);

        if (this.descriptionText)
            infoButton(this.svg, -this.margin.left + 12, 0, this.descriptionText);

        // Add legend container inside the chart area (upper right corner)
        this.legend = this.svg.append("g")
            .attr("class", "legend")
            // .attr("transform", `translate(${this.innerWidth / 2}, 10)`); // Position inside the upper right corner
            .attr("transform", `translate(10, 10)`);
    }

    removeChart()
    {
        if (this.tippy)
            this.tippy.destroy();
        this.rootSvg.remove();
    }

    clear()
    {
        this.removeChart();
        this.initChart();
    }

    addSeries(name, on_click = null)
    {
        const colorIndex = this.series.length % this.colors.length;
        const newSeries = {
            seriesIndex: this.series.length,
            data: [],
            line: d3.line()
                .x(d => this.xScale(d.x))
                .y(d => this.yScale(d.y)),
            path: this.svg.append("path")
                .datum([])
                .attr("fill", "none")
                .attr("stroke", this.colors[colorIndex])
                .attr("stroke-width", 2),
            points: null // Placeholder for the points
        };

        // Add addPoint method to the series
        newSeries.addPoint = (newPoint) => {
            // Add the new point to the series data array
            newSeries.data.push(newPoint);

            // Sort the data by x values
            //newSeries.data.sort((a, b) => a.x - b.x);

            // Update scales
            const xDomain = [
                d3.min(this.series.flatMap(s => s.data), d => d.x),
                d3.max(this.series.flatMap(s => s.data), d => d.x),
            ];
            const yDomain = [
                d3.min(this.series.flatMap(s => s.data), d => d.y),
                d3.max(this.series.flatMap(s => s.data), d => d.y),
            ];
            this.xScale.domain(xDomain).nice();
            this.yScale.domain(yDomain).nice();

            // Update all series lines and points
            this.series.forEach(series => {
                // Update line
                series.path.datum(series.data)
                    .attr("d", series.line);

                // Update points
                if (series.points)
                    series.points.remove();

                series.points = this.svg.selectAll(`.point-series-${series.seriesIndex}`)
                    .data(series.data)
                    .enter()
                    .append("circle")
                    .attr("class", `point-series-${series.seriesIndex}`)
                    .attr("cx", d => this.xScale(d.x))
                    .attr("cy", d => this.yScale(d.y))
                    .attr("r", 2)
                    .attr("fill", this.colors[series.seriesIndex % this.colors.length])
                    .on("click", (event, d) => {
                        // Highlight last clicked point
                        this.svg.selectAll("circle").attr("stroke", null).attr("stroke-width", null); // Remove highlight from all points
                        d3.select(event.target).attr("stroke", "black").attr("stroke-width", 2); // Highlight clicked point
                        if (on_click)
                            on_click(d, false);
                    });
            });

            // Update axes
            this.svg.select(".x-axis")
                .call(d3.axisBottom(this.xScale));

            this.svg.select(".y-axis")
                .call(d3.axisLeft(this.yScale));

            if (this.min_track) {
                // Indicate point with minimum y value
                const minPoint = newSeries.data.reduce((min, d) => d.y < min.y ? d : min, newSeries.data[0]);
                if (newSeries.arrow)
                    newSeries.arrow.remove();

                // Add text below the arrow with x-value
                if (newSeries.arrowText)
                    newSeries.arrowText.remove();

                newSeries.arrow = this.svg.append("line")
                    .attr("x2", this.xScale(minPoint.x))
                    .attr("y2", this.yScale(minPoint.y) + (newSeries.seriesIndex * 15) + 10) // Add a gap of 10 pixels
                    .attr("x1", this.xScale(minPoint.x))
                    .attr("y1", this.yScale(minPoint.y) + ((newSeries.seriesIndex + 1) * 15) + 10) // Add a gap of 10 pixels
                    .attr("stroke", this.colors[colorIndex])
                    .attr("stroke-width", 2);

                newSeries.arrowText = this.svg.append("text")
                    .attr("x", this.xScale(minPoint.x))
                    .attr("y", this.yScale(minPoint.y) + ((newSeries.seriesIndex + 1) * 15) + 25) // Add a gap of 10 pixels below the arrow
                    .attr("text-anchor", "middle")
                    .style("font-size", "10px")
                    .text(`x: ${minPoint.x.toFixed(1)}, y: ${minPoint.y.toFixed(1)}`);

                // Track global min Y value and auto-click it
                if (on_click && this.minY > newPoint.y) {
                    this.minY = newPoint.y;
                    on_click(newPoint, true);
                }
            }
        };

        // Add legend entry
        const legendItem = this.legend.append("g")
            .attr("class", "legend-item")
            .attr("transform", `translate(${(this.series.length % 5) * 100}, ${Math.floor(this.series.length / 5) * 20})`);

        legendItem.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 18)
            .attr("height", 18)
            .style("fill", this.colors[colorIndex])
            .style("opacity", 1.0)
            .style("cursor", "pointer")
            .on("click", (event, d) => {
                function toggleVisibility(selection) {
                    selection.style("visibility", (d, i, nodes) =>
                        d3.select(nodes[i]).style("visibility") === "hidden" ? "visible" : "hidden"
                    );
                }
                function toggleOpacity(element) {
                    element.style("opacity", (d, i, nodes) =>
                        d3.select(nodes[i]).style("opacity") === "1" ? 0.2 : 1.0
                    );
                }
                toggleVisibility(this.svg.selectAll(`.point-series-${newSeries.seriesIndex}`));
                toggleVisibility(newSeries.path);
                toggleOpacity(d3.select(event.target));
            });

        legendItem.append("text")
            .attr("x", 24)
            .attr("y", 9)
            .attr("dy", "0.15em")
            .text(name)
            .style("font-size", "12px")
            .style("alignment-baseline", "middle");

        this.series.push(newSeries);
        return newSeries;
    }
}

// HTML integration example
/*
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chart Module Example</title>
    <script type="module" src="chart.js"></script>
    <script src="https://d3js.org/d3.v7.min.js"></script>
</head>
<body>
    <div id="chart-container"></div>
    <script type="module">
        import { Chart } from './chart.js';

        const chartSelection = d3.select("#chart-container");
        const chart = new Chart(chartSelection, "Time (s)", "Value", "This chart shows the relationship between time and value."); // Example of setting x and y axis titles and a description
        const series1 = chart.addSeries('Series 1', (data) => console.log('Clicked on:', data));
        const series2 = chart.addSeries('Series 2');

        series1.addPoint({x: 1, y: 10, info: 'Point A' });
        series1.addPoint({x: 2, y: 15, info: 'Point B' });
        series2.addPoint({x: 1, y: 5,  info: 'Point C' });
        series2.addPoint({x: 2, y: 8,  info: 'Point D' });
    </script>
</body>
</html>
*/