import { MergeTreeUtilityVisualizer, MergeTreeTimeVisualizer } from './MergeTreeVisualizer.js';
import { MergeTreeRewinder } from './MergeTreeRewinder.js';

export class SimulationContainer {
    constructor() {
        this.visualizers = null;
        this.rewinder = null;
    }

    #showMetrics(mt)
    {
        // Append text with metrics
        d3.select("#metrics-container")
            .text(`
                ${mt.title === undefined ? "" : mt.title}
                Total: ${mt.written_bytes / 1024 / 1024} MB,
                Inserted: ${mt.inserted_bytes / 1024 / 1024} MB,
                WA: ${mt.writeAmplification().toFixed(2)},
                AvgPartCount: ${mt.avgActivePartCount().toFixed(2)},
                Time: ${(mt.time).toFixed(2)}s,
                Integral: ${mt.integral_active_part_count.toFixed(2)}
            `);
    }

    #init(data, automatic = false)
    {
        if (automatic && window.__pause)
            return;
        if (!automatic)
            console.log("SHOW", data);
        const {mt} = data;
        if (mt !== undefined)
        {
            this.visualizers = {
                util: new MergeTreeUtilityVisualizer(mt, d3.select("#util-container")),
                time: new MergeTreeTimeVisualizer(mt, d3.select("#time-container")),
            }
            this.rewinder = new MergeTreeRewinder(mt, this.visualizers, d3.select("#rewind-container"));
            this.#showMetrics(mt);
        }
    }

    update(data, automatic = false)
    {
        if (automatic && window.__pause)
            return;
        if (!this.visualizers) {
            this.#init(data, automatic);
        } else {
            const {mt} = data;
            if (!automatic)
                console.log("SHOW UPDATE", data);
            for (const v of Object.values(this.visualizers))
                v.update();
            this.rewinder.update();
            this.#showMetrics(mt);
        }
    }

    setTime(time)
    {
        this.rewinder.setTime(time);
    }
}
