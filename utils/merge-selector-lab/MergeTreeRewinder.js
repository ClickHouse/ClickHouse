import { MergeTreeUtilityVisualizer, MergeTreeTimeVisualizer } from './MergeTreeVisualizer.js';

// Component that renders a scroll bar for MergeTree that rewinds time and controls
// MergeTreeVisualizer(s)
export class MergeTreeRewinder {
    constructor(mt, visualizers, container) {
        this.mt = mt;
        this.visualizers = visualizers;
        this.time = mt.time;
        this.minTime = 0;
        this.maxTime = mt.time;
        this.container = container;

        // Render the initial slider
        this.speedMultipliers = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000];
        this.currentSpeedIndex = 0;
        this.renderSlider();
    }

    stepBackward() {
        if (this.isPlaying) {
            this.togglePlay(); // Stop playback if playing
        }
        this.time = Math.max(this.time - this.speed, this.minTime);
        this.onTimeSet(this.time);
        this.container.select("input").property("value", this.time);
    }

    stepForward() {
        if (this.isPlaying) {
            this.togglePlay(); // Stop playback if playing
        }
        this.time = Math.min(this.time + this.speed, this.maxTime);
        this.onTimeSet(this.time);
        this.container.select("input").property("value", this.time);
    }

    toggleSpeed() {
        this.currentSpeedIndex = (this.currentSpeedIndex + 1) % this.speedMultipliers.length;
        this.speedButton.text(`x${this.speedMultipliers[this.currentSpeedIndex]}`);
        this.speed = 0.1 * this.speedMultipliers[this.currentSpeedIndex];
    }

    togglePlay() {
        if (this.isPlaying) {
            clearInterval(this.playInterval);
            this.isPlaying = false;
            this.playButton.text("▶️");
        } else {
            this.isPlaying = true;
            this.playButton.text("⏸️");
            this.playInterval = setInterval(() => {
                if (this.time < this.maxTime) {
                    this.time = Math.min(this.time + this.speed, this.maxTime);
                    this.onTimeSet(this.time);
                    this.container.select("input").property("value", this.time);
                    this.container.select("input").attr("value", this.time);
                } else {
                    this.togglePlay(); // Stop at maxTime
                }
            }, 100);
        }
    }

    updateLabel() {
        const days = Math.floor(this.time / 60 / 60 / 24);
        const hours = Math.floor((this.time / 60 / 60) % 24).toString().padStart(2, '0');
        const minutes = Math.floor((this.time / 60) % 60).toString().padStart(2, '0');
        const seconds = Math.floor(this.time % 60).toString().padStart(2, '0');
        const fraction = (this.time % 1).toFixed(2).substring(2);
        let text = `${minutes}:${seconds}.${fraction}`;
        if (hours != 0 || days != 0)
            text = `${hours}:${text}`;
        if (days != 0)
            text = `${days}d${text}`;
        this.label.text(text);
    }

    setMinTime(value) {
        this.minTime = value;
        if (this.time < this.minTime) {
            this.time = this.minTime;
        }
        this.update();
    }

    // Render a slider [this.minTime, this.maxTime] using HTML in `container` element with callback to this.onTimeSet
    renderSlider() {
        // Clear any existing content
        this.container.html("");

        // Create speed multiplier button
        this.speedButton = this.container.append("button")
            .attr("class", "btn btn-secondary btn-sm mr-1")
            .style("padding", "2px 4px")
            .style("border", "none")
            .style("color", "white")
            .text(`x${this.speedMultipliers[this.currentSpeedIndex]}`)
            .on("click", () => this.toggleSpeed());

        // Create step backward button
        this.stepBackButton = this.container.append("button")
            .attr("class", "btn btn-sm mr-1")
            .style("padding", "0")
            .style("background", "transparent")
            .style("border", "none")
            .style("color", "inherit")
            .text("⏪")
            .on("click", () => this.stepBackward());

        // Create play/pause button
        this.isPlaying = false;
        this.speed = 0.1;
        this.playButton = this.container.append("button")
            .attr("class", "btn btn-sm mr-1")
            .style("padding", "0")
            .style("background", "transparent")
            .style("border", "none")
            .style("color", "inherit")
            .text("▶️")
            .on("click", () => this.togglePlay());

        // Create step forward button
        this.stepForwardButton = this.container.append("button")
            .attr("class", "btn btn-sm mr-1")
            .style("padding", "0")
            .style("background", "transparent")
            .style("border", "none")
            .style("color", "inherit")
            .text("⏩")
            .on("click", () => this.stepForward());

        // Create slider input
        this.slider = this.container.append("input")
            .attr("type", "range")
            .attr("min", this.minTime)
            .attr("max", this.maxTime)
            .attr("value", this.time)
            .attr("class", "form-range w-75 slider-custom")
            .style("background", "#f8f9fa")
            .style("border", "1px solid #ced4da")
            .style("border-radius", "5px")
            .style("height", "8px")
            .style("accent-color", "#ffc107")
            .attr("step", 0.1)
            .style("vertical-align", "middle")
            .on("input", (event) => this.onTimeSet(+event.target.value));

        // Create a label to show the current time value
        this.label = this.container.append("span")
            .style("display", "inline-block")
            .style("vertical-align", "middle")
            .style("line-height", "1")
            .attr("class", "text-white ml-2");
        this.updateLabel();
    }

    // Called during simulation to update max time and re-render the slider
    update() {
        if (this.time == this.maxTime) {
            this.time = this.mt.time; // Stick to the right mode
        }
        this.maxTime = this.mt.time;

        // Update the slider attributes and label
        this.container.select("input")
            .attr("max", this.maxTime)
            .attr("min", this.minTime)
            .property("value", this.time);

        this.updateLabel();
    }

    // Called when slider position is changed to update visualizations
    onTimeSet(time) {
        this.time = time;
        this.updateLabel();
        for (const v of Object.values(this.visualizers)) {
            v.setPartFilter((d) => d.created <= time);
            v.setTime(time);
            v.update();
        }
    }

    setTime(time) {
        if (this.isPlaying) {
            this.togglePlay(); // Stop playback if playing
        }
        this.time = time;
        this.onTimeSet(this.time);
        this.container.select("input").property("value", this.time);
    }
}

/* index.html:
<!DOCTYPE html>
<html>
    <head>
        <title>Merge Selector Lab</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        <script src="https://d3js.org/d3.v7.min.js"></script>
    </head>
    <body>
        <div class="container-fluid bg-primary text-white">
            <div class="p-3" id="metrics-container"></div>
        </div>

        <div class="container-fluid">
            <div class="row">
                <div class="col-md-12" id="util-container"></div>
            </div>
        </div>

        <div class="container-fluid bg-primary text-white">
            <div class="p-3" id="rewind-container"></div>
        </div>

        <div class="container-fluid">
            <div class="row">
                <div class="col-md-12" id="time-container"></div>
            </div>
        </div>

        <div class="container-fluid">
            <div class="p-3" id="var-container"></div>
        </div>

        <script type="module">
            import { MergeTree } from './MergeTree.js';
            import { MergeTreeRewinder } from './MergeTreeRewinder.js';
            import { MergeTreeUtilityVisualizer, MergeTreeTimeVisualizer } from './MergeTreeVisualizer.js';

            const mt = new MergeTree();
            const visualizers = {
                util: new MergeTreeUtilityVisualizer(mt, d3.select("#util-container")),
                time: new MergeTreeTimeVisualizer(mt, d3.select("#time-container")),
            };
            const rewinder = new MergeTreeRewinder(mt, visualizers, d3.select("#rewind-container"));

            // Simulate updates (replace this with actual simulation logic)
            setInterval(() => {
                mt.time += 1;
                rewinder.update();
            }, 1000);
        </script>
    </body>
</html>
*/
