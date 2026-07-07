function targetSizesForBases(insertPartSize, layerBases)
{
    let sizes = [insertPartSize];
    for (const base of layerBases)
    {
        const prev = sizes[sizes.length - 1];
        sizes.push(prev * base);
    }
    return sizes;
}

function layerFuncForBases(sizes)
{
    let boundaries = [];
    for (let i = 1; i < sizes.length; i++)
    {
        const lo = sizes[i - 1];
        const hi = sizes[i];
        boundaries.push(Math.sqrt(lo * hi)); // Geometric mean
    }

    // console.log("BOUNDARIES", boundaries);

    return (size) => {
        for (let i = 0; i < boundaries.length; i++)
        {
            if (size < boundaries[i])
                return i;
        }
        return boundaries.length;
    }
}

// Returns true iff most of [left, right) interval is contained in [target_left, target_right) intervals
function contained(left, right, target_left, target_right)
{
    const intersection_left = Math.max(left, target_left);
    const intersection_right = Math.min(right, target_right);
    const intersection = intersection_right - intersection_left; // could be negative
    const length = right - left;
    if (2 * intersection > length)
        return true;
    if (2 * intersection == length)
        return left < target_left; // tie breaker
    return false;
}

function partContained(part, target_left, target_right)
{
    return contained(part.left_bytes, part.right_bytes, target_left, target_right);
}

// Assigns merges based on size of source parts by "layers"
// Part layer is determined by its size with `layerFunc(size)`:
//   layer 0: smallest parts
//   layer k: parts of prev layer size multiplied by layerBases[k - 1]
//   layer L: max size parts
// Can merges parts of different layers.
// Resulting "target" size of part is determined by `bases[0..L]` array.
// Part alignment is done based on layer and it's "target" size.
// (Alignment is important to avoid fragmentation)
// Lower resulting part sizes get absolute priority over higher ones.
export function* floatLayerMerges({insertPartSize, layerBases, mergeRight})
{
    const sizes = targetSizesForBases(insertPartSize, layerBases);
    const layerFunc = layerFuncForBases(sizes);
    const mt = yield {type: 'getMergeTree'};

    const settings = {
        max_parts_to_merge_at_once: 0,
    };

    while (true)
    {
        let best_begin = -1;
        let best_end = -1;
        let best_layer = layerBases.length;
        const active_parts = mt.sortedActiveParts();
        for (let begin = 0; begin <= active_parts.length - 2; begin++)
        {
            const prev_part = begin == 0 ? null : active_parts[begin - 1];
            const begin_part = active_parts[begin];
            if (begin_part.merging)
                continue;
            const layer = layerFunc(begin_part.bytes);
            // Only check merges of parts of the same layer
            if (layer < best_layer)
            {
                const target = sizes[layer + 1]; // Approximate target size of resulting part

                // Check alignment with beginning of target interval
                const target_lo = Math.floor(begin_part.left_bytes / target) * target; // 1st boundary to the left of `left`
                const target_mi = target_lo + target; // 1st boundary to the right of `left`
                const target_hi = target_mi + target; // 2nd boundary to the right of `left`

                // There are three options:
                // 1) begin_part is the first part of [target_lo, target_mi]
                // 2) begin_part is the first part of [target_mi, target_hi]
                // 3) none of the above
                let target_left;
                let target_right;
                if (partContained(begin_part, target_lo, target_mi)
                    && (!prev_part || !partContained(prev_part, target_lo, target_mi)))
                {
                    target_left = target_lo;
                    target_right = target_mi;
                }
                else if (partContained(begin_part, target_mi, target_hi)
                    && (!prev_part || !partContained(prev_part, target_mi, target_hi)))
                {
                    target_left = target_mi;
                    target_right = target_hi;
                }
                else
                {
                    // Option 3. It is not the first part on the target interval -- skip
                    continue;
                }

                let allowed = false;
                let end = begin + 1;
                let max_right_bytes = begin_part.right_bytes;
                for (; end < active_parts.length; end++)
                {
                    const part = active_parts[end];
                    max_right_bytes = part.right_bytes;
                    if (!partContained(part, target_left, target_right))
                    {
                        // Add last part on the layer to the last merge on the layer
                        if (!mergeRight || end < active_parts.length - 1)
                        {
                            allowed = true; // We reached end of target interval
                            break;
                        }
                    }
                    if (part.merging)
                        break;
                    const partLayer = layerFunc(part.bytes);
                    if (  (!mergeRight && partLayer != layer)
                        || (mergeRight && partLayer > layer))
                        break;
                }

                // Special conditions on the rightmost boundary of active parts
                // We dont have next part to check that if falls out of target interval, so we use fake future inserted part of size `insertPartSize`
                if (end == active_parts.length)
                {
                    const future_left = max_right_bytes;
                    const future_right = max_right_bytes + insertPartSize;
                    if (mergeRight || !contained(future_left, future_right, target_left, target_right))
                    {
                        // Most probably next inserted part will be not contained in the target interval
                        allowed = true;
                    }
                }

                // Validate number of parts to merge
                let part_count = end - begin;
                if (part_count < 2)
                    continue; // Too few parts to merge
                if (settings.max_parts_to_merge_at_once && part_count > settings.max_parts_to_merge_at_once)
                    continue; // Too many parts to merge at once

                if (allowed)
                {
                    best_begin = begin;
                    best_end = end;
                    best_layer = layer;
                }
            }
            if (best_layer == 0)
                break; // shortcut
        }
        if (best_layer != layerBases.length)
        {
            // console.log("SELECTED", best_end - best_begin, {best_begin, best_end, parts_to_merge: active_parts.slice(best_begin, best_end)});
            yield {type: 'merge', parts_to_merge: active_parts.slice(best_begin, best_end)};
        }
        else
        {
            // console.log("SELECTOR WAIT");
            yield {type: 'wait'};
        }
    }
}
