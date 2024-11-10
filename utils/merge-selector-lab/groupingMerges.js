export function groupingMerges(mt, {count})
{
    let settings =
    {
        max_parts_to_merge_at_once: 100,
        min_parts: 2,
        max_part_size: 150 * 1024 * 1024 * 1024,
        target_wa: 4,
    };

    const sumBytes = parts_array => d3.sum(parts_array, d => d.bytes);
    const sumUtility = parts_array => d3.sum(parts_array, d => d.utility);

    // NOTE: utility = sum_over_range_of_parts(part.bytes * log(part.bytes))
    const makeNode = (begin, end, total_bytes, total_utility) => ({begin, end, total_bytes, total_utility});

    function partitionNodeMax(node, active_parts, max_child_bytes)
    {
        let begin = 0;
        let sum_bytes = 0;
        let sum_utility = 0;
        node.children = [];
        for (let i = node.begin; i < node.end; i++)
        {
            let part = active_parts[i];
            if (begin != i && sum_bytes + part.bytes > max_child_size)
            {
                node.children.push(makeNode(begin, i, sum_bytes, sum_utility));
                begin = i;
                sum_bytes = 0;
                sum_utility = 0;
            }
            sum_bytes += part.bytes;
            sum_utility += part.bytes * part.log_bytes
        }
        node.children.push(makeNode(begin, i, sum_bytes, sum_utility));
    }

    function partitionNodeMin(node, active_parts, min_child_size)
    {
        let begin = 0;
        let sum_bytes = 0;
        let sum_utility = 0;
        node.children = [];
        for (let i = node.begin; i < node.end; i++)
        {
            if (begin != i && sum_bytes > min_child_size)
            {
                node.children.push(makeNode(begin, i, sum_bytes, sum_utility));
                begin = i;
                sum_bytes = 0;
                sum_utility = 0;
            }
            let part = active_parts[i];
            sum_bytes += part.bytes;
            sum_utility += part.bytes * part.log_bytes
        }
        // NOTE: the the last group could be less `min_child_size`
        // Consider NOT merging this group on its own, because it leads to higher WA
        // If it is the last group - DO NOT MERGE, wait for more inserts
        // Otherwise, merge it with top, left or right group
        // Use utility/bytes to choose
        node.children.push(makeNode(begin, i, sum_bytes, sum_utility));
    }

    for (let merge_num = 0; merge_num < count; merge_num++)
    {
        const active_parts = mt.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
        if (active_parts.length < 2)
            return;
        const total_bytes = sumBytes(active_parts);
        const total_utility = sumUtility(active_parts);
        let root = makeNode(0, active_parts.length, total_bytes, total_utility);
        partitionNodeMax(root, active_parts, settings.max_part_size);
        const utility_zero = d3.sum(root.children, d => d.total_bytes * Math.log2(d.total_bytes));
        const avg_height = (utility_zero - inserted_utility) / (mt.inserted_bytes * (target_wa - 1));
        let height = Math.log2(mt.inserted_bytes);
        let layer_nodes = root.children;
        while (true) {
            const min_child_size = Math.pow(2, height);
            let progress = false;
            for (let i = 0; i < layer_nodes.length; i++)
            {
                let node = layer_nodes[i];
                if (node.end - node.begin > min_parts)
                {
                    // TODO
                }
            }
            height -= avg_height;
        }
    }
}
