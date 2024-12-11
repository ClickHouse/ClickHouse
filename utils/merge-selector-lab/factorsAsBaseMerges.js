export function factorsAsBaseMerges(mt, {count, factors})
{
    factors = factors.slice(); // clone array
    let check = factors.reduce((acc, val) => acc * val, 1);
    if (check != count)
        throw { message: "Wrong factorization", product: count, factors: factors};
    let merge_count = count;
    let base = factors.shift();
    merge_count /= base;
    let merge_left = merge_count;
    for (let i = 0; i < count; i++)
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
        mt.mergeParts(eligible_parts.slice(0, base));
        if (--merge_left == 0)
        {
            if (factors.length == 0)
                break;
            base = factors.shift();
            merge_count /= base;
            merge_left = merge_count;
        }
    }
}
