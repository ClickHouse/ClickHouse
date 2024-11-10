////////////////////////////////////////////////////////////////////////////////
// Simulation of a merge tree with INSERTs/MERGEs in time
//
export class MergeTree {
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
            merging: false,
            idx: this.parts.length,
            source_part_count: 1
        };
        this.parts.push(result);
        this.active_part_count++;
        //console.log("INSERT", result);
        this.inserted_parts_count++;
        this.inserted_bytes += bytes;
        this.written_bytes += bytes;
        this.inserted_utility += utility;
        return result;
    }

    // Execute merge
    mergeParts(parts_to_merge)
    {
        // Compute time required for merge
        const bytes = parts_to_merge.reduce((sum, part) => sum + part.bytes, 0);
        const merge_duration = this.mergeDuration(bytes, parts_to_merge.length);
        this.beginMergeParts(parts_to_merge);
        this.advanceTime(this.current_time + merge_duration);
        return this.finishMergeParts(parts_to_merge);
    }

    beginMergeParts(parts_to_merge)
    {
        // Mark parts as participating in a merge
        for (let p of parts_to_merge)
        {
            if (p.active == false)
                throw { message: "Attempt to begin merge of inactive part", part: p};
            if (p.merging == true)
                throw { message: "Attempt to begin merge of part that already participates in another merge", part: p};
            p.merging = true;
        }
    }

    finishMergeParts(parts_to_merge)
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
        const bytes = parts_to_merge.reduce((sum, part) => sum + part.bytes, 0);
        const utility0 = parts_to_merge.reduce((sum, part) => sum + part.utility, 0);
        const log_bytes = Math.log2(bytes);
        const utility = bytes * log_bytes;
        const entropy = (utility - utility0) / bytes;

        let result = {
            bytes,
            log_bytes,
            utility,
            entropy,
            created: this.current_time,
            level: 1 + Math.max(...parts_to_merge.map(d => d.level)),
            begin: Math.min(...parts_to_merge.map(d => d.begin)),
            end: Math.max(...parts_to_merge.map(d => d.end)),
            active: true,
            merging: false,
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
            p.merging = false;
            this.active_part_count--;
        }
        //console.log("MERGE", parts_to_merge, "INTO", result);
        this.written_bytes += bytes;
        return result;
    }
}
