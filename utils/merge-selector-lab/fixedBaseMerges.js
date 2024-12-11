export function* fixedBaseMerges({base})
{
    // TODO: adapt for parallel merges
    let mt = yield {type: 'getMergeTree'};
    while (true)
    {
        if (mt.active_part_count <= 1)
            break;
        const active_parts = mt.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
        let max_size = 0;
        let eligible_parts = [];
        while (eligible_parts.length < Math.min(mt.active_part_count, base))
        {
            max_size = d3.min(active_parts.filter(d => d.bytes > max_size), d => d.bytes);
            eligible_parts = active_parts.filter(d => d.bytes <= max_size);
        }
        yield {type: 'merge', parts_to_merge: eligible_parts.slice(0, base)};
    }
}
