export function* floatBaseMerges({base})
{
    // TODO: adapt for parallel merges
    let mt = yield {type: 'getMergeTree'};
    while (true)
    {
        if (mt.active_part_count <= 1)
            break;
        const active_parts = mt.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
        const target_size = base * mt.inserted_bytes / mt.active_part_count;

        let sum_bytes = 0;
        let boundary = target_size;
        let parts_to_merge = [];
        for (const part of active_parts)
        {
            const left = sum_bytes;
            const right = sum_bytes + part.bytes;
            if (parts_to_merge.length < 2 || right < boundary || (left < boundary && boundary - left >= right - boundary))
            {
                sum_bytes += part.bytes;
                parts_to_merge.push(part);
                continue;
            }

            yield {type: 'merge', parts_to_merge};

            sum_bytes += part.bytes;
            parts_to_merge = [part];
            boundary += target_size;
        }

        if (parts_to_merge.length >= 2)
            yield {type: 'merge', parts_to_merge};
    }
}
