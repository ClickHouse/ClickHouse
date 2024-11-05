export function fixedBaseMerges(sim, {count, base})
{
    for (let i = 0; i < count; i++)
    {
        if (sim.active_part_count <= 1)
            break;
        const active_parts = sim.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
        let max_size = 0;
        let eligible_parts = [];
        while (eligible_parts.length < Math.min(sim.active_part_count, base))
        {
            max_size = d3.min(active_parts.filter(d => d.bytes > max_size), d => d.bytes);
            eligible_parts = active_parts.filter(d => d.bytes <= max_size);
        }
        sim.mergeParts(eligible_parts.slice(0, base));
    }
}
