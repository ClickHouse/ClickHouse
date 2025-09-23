// Simulation of a merge tree with INSERTs/MERGEs in time
export class MergeTree {
    constructor()
    {
        // Append only array of all parts active and outdated
        this.parts = [];

        // Metrics
        this.time = 0;
        this.total_part_count = 0;
        this.inserted_part_count = 0;
        this.inserted_bytes = 0;
        this.written_bytes = 0; // inserts + merges
        this.inserted_utility = 0; // utility = size * log(size)
        this.active_part_count = 0;
        this.merging_part_count = 0;
        this.integral_active_part_count = 0;

        // Observers
        this.insert_observers = [];
    }

    addInsertObserver(observer)
    {
        this.insert_observers.push(observer);
    }

    writeAmplification()
    {
        return this.written_bytes / this.inserted_bytes;
    }

    avgActivePartCount()
    {
        return this.integral_active_part_count / this.time;
    }

    mergeDuration(bytes, parts)
    {
        const merge_speed_bps = (100 << 20); // 100MBps
        return bytes / merge_speed_bps + parts * 0.0;
    }

    advanceTime(now)
    {
        if (this.time > now)
            throw { message: "Times go backwards", from: this.time, to: now};
        const time_delta = now - this.time;
        this.integral_active_part_count += this.active_part_count * time_delta;
        this.time = now;
    }

    // Inserts a new part
    insertPart(bytes, now = this.time)
    {
        this.advanceTime(now);
        let log_bytes = Math.log2(bytes);
        let utility = bytes * log_bytes;
        let result = {
            id: this.total_part_count,
            bytes,
            log_bytes,
            utility,
            entropy: 0,
            created: this.time,
            level: 0,
            begin: this.inserted_part_count,
            end: this.inserted_part_count + 1,
            left_bytes: this.inserted_bytes,
            right_bytes: this.inserted_bytes + bytes,
            is_leftmost: false,
            is_rightmost: false,
            active: true,
            merging: false,
            parent_started: Infinity,
            idx: this.parts.length,
            source_part_count: 0
        };
        this.parts.push(result);
        this.active_part_count++;
        this.inserted_part_count++;
        this.total_part_count++;
        this.inserted_bytes += bytes;
        this.written_bytes += bytes;
        this.inserted_utility += utility;
        //console.log("INSERT", result);

        for (const observer of this.insert_observers)
            observer(result);

        return result;
    }

    // Execute merge (for single merge worker models only)
    mergeParts(parts_to_merge)
    {
        // Compute time required for merge
        const bytes = parts_to_merge.reduce((sum, part) => sum + part.bytes, 0);
        const merge_duration = this.mergeDuration(bytes, parts_to_merge.length);
        this.beginMergeParts(parts_to_merge);
        this.advanceTime(this.time + merge_duration);
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
            p.parent_started = this.time;
            this.merging_part_count++;
        }
        // console.log("BEGIN MERGE", this.time, parts_to_merge);
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
            id: this.total_part_count,
            bytes,
            log_bytes,
            utility,
            entropy,
            created: this.time,
            started: this.time - this.mergeDuration(bytes, parts_to_merge.length),
            level: 1 + Math.max(...parts_to_merge.map(d => d.level)),
            begin: Math.min(...parts_to_merge.map(d => d.begin)),
            end: Math.max(...parts_to_merge.map(d => d.end)),
            left_bytes: Math.min(...parts_to_merge.map(d => d.left_bytes)),
            right_bytes: Math.max(...parts_to_merge.map(d => d.right_bytes)),
            active: true,
            merging: false,
            parent_started: Infinity,
            idx: this.parts.length,
            source_part_count: parts_to_merge.length,
        };
        this.parts.push(result);
        this.active_part_count++;
        this.total_part_count++;
        for (let p of parts_to_merge)
        {
            if (p.active == false)
                throw { message: "Merging inactive part", part: p};

            // Update children parts
            p.parent = idx;
            p.parent_part = result;
            p.active = false;
            p.merging = false;
            if (p.left_bytes == result.left_bytes)
                p.is_leftmost = true;
            if (p.right_bytes == result.right_bytes)
                p.is_rightmost = true;

            // Update metrics
            this.merging_part_count--;
            this.active_part_count--;
        }
        // console.log("END MERGE", this.time, parts_to_merge, "INTO", result);
        this.written_bytes += bytes;
        return result;
    }

    sortedActiveParts()
    {
        // TODO(serxa): optimize it by maintaining this array dynamically with merges and inserts
        return this.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
    }

    // Returns all ranges to be considered by merge selector (excluding already merging parts)
    getRangesForMerge()
    {
        const active_parts = this.sortedActiveParts();
        const all_ranges = [];
        let cur_range = [];
        for (const p of active_parts)
        {
            if (p.merging)
            {
                if (cur_range.length > 1) // There is no point in ranges with the only part
                    all_ranges.push(cur_range);
                cur_range = [];
            }
            else
            {
                cur_range.push(p);
            }
        }
        if (cur_range.length > 1) // There is no point in ranges with the only part
            all_ranges.push(cur_range);
        return all_ranges;
    }
}
