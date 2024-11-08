////////////////////////////////////////////////////////////////////////////////
// Simulation of a merge tree with INSERTs/MERGEs in time
//
export class Simulator {
    constructor()
    {
        // Append only array of all parts active and outdated
        this.parts = [];

        // Metrics
        this.current_time = 0;
        this.inserted_parts_count = 0;
        this.inserted_bytes = 0;
        this.written_bytes = 0; // inserts + merges
        this.inserted_utility = 0; // utility = size * log(size)
        this.active_part_count = 0;
        this.integral_active_part_count = 0;
    }

    writeAmplification()
    {
        return this.written_bytes / this.inserted_bytes;
    }

    avgActivePartCount()
    {
        return this.integral_active_part_count / this.current_time;
    }

    mergeDuration(bytes, parts)
    {
        const merge_speed_bps = (100 << 20); // 100MBps
        return bytes / merge_speed_bps + parts * 0.0;
    }

    advanceTime(now)
    {
        if (this.current_time > now)
            throw { message: "Times go backwards", from: this.current_time, to: now};
        const time_delta = now - this.current_time;
        this.integral_active_part_count += this.active_part_count * time_delta;
        this.current_time = now;
    }

    // Inserts a new part
    insertPart(bytes, now = this.current_time)
    {
        this.advanceTime(now);
        let log_bytes = Math.log2(bytes);
        let utility = bytes * log_bytes;
        let result = {
            bytes,
            log_bytes,
            utility,
            entropy: 0,
            created: now,
            level: 0,
            begin: this.inserted_parts_count,
            end: this.inserted_parts_count + 1,
            active: true,
            idx: this.parts.length,
            source_part_count: 1
        };
        this.parts.push(result);
        this.active_part_count++;
        console.log("INSERT", result);
        this.inserted_parts_count++;
        this.inserted_bytes += bytes;
        this.written_bytes += bytes;
        this.inserted_utility += utility;
        return result;
    }

    // Execute merge
    mergeParts(parts_to_merge)
    {
        // Validation
        if (parts_to_merge.length < 2)
            throw { message: "Unable to merge", parts_to_merge: parts_to_merge};
        let prev = null;
        for (let next of parts_to_merge)
        {
            if (prev != null && prev.end != next.begin)
                throw { message: "Merging NOT adjacent parts", prev, next};
            prev = next;
        }

        const idx = this.parts.length;
        const bytes = d3.sum(parts_to_merge, d => d.bytes);
        const utility0 = d3.sum(parts_to_merge, d => d.utility);
        const log_bytes = Math.log2(bytes);
        const utility = bytes * log_bytes;
        const entropy = (utility - utility0) / bytes;

        // Compute time required for merge
        const merge_duration = this.mergeDuration(bytes, parts_to_merge.length);
        this.advanceTime(this.current_time + merge_duration);

        let result = {
            bytes,
            log_bytes,
            utility,
            entropy,
            created: this.current_time,
            level: 1 + d3.max(parts_to_merge, d => d.level),
            begin: d3.min(parts_to_merge, d => d.begin),
            end: d3.max(parts_to_merge, d => d.end),
            active: true,
            idx: this.parts.length,
            source_part_count: parts_to_merge.length,
        };
        this.parts.push(result);
        this.active_part_count++;
        for (let p of parts_to_merge)
        {
            p.parent = idx;
            if (p.active == false)
                throw { message: "Merging inactive part", part: p};
            p.active = false;
            this.active_part_count--;
        }
        console.log("MERGE", parts_to_merge, "INTO", result);
        this.written_bytes += bytes;
        return result;
    }
}
