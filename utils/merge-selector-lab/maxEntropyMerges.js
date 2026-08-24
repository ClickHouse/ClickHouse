export function* maxEntropyMerges({min_parts, max_parts, min_score})
{
    // TODO: adapt for parallel merges
    let mt = yield {type: 'getMergeTree'};
    while (true)
    {
        const active_parts = mt.parts.filter(d => d.active).sort((a, b) => a.begin - b.begin);
        let best_begin = -1;
        let best_end = -1;
        let best_score = 0;
        for (let begin = 0; begin < active_parts.length - min_parts; begin++)
        {
            for (let end = begin + min_parts; end < active_parts.length; end++)
            {
                if (end - begin > max_parts)
                    break;
                const range = active_parts.slice(begin, end);
                const sum_bytes = d3.sum(range, d => d.bytes);
                const log_sum_bytes = Math.log2(sum_bytes);
                const score = d3.sum(range, d => d.bytes * (log_sum_bytes - d.log_bytes)) / sum_bytes;
                if (score > best_score)
                {
                    best_begin = begin;
                    best_end = end;
                    best_score = score;
                }
            }
        }
        if (best_score > min_score)
            yield {type: 'merge', parts_to_merge: active_parts.slice(best_begin, best_end)};
        else
            yield {type: 'wait'}; // Better not to merge
    }
}
