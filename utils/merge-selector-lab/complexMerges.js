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

function layerFuncForBases({solver, insertPartSize, workers})
{
    let boundaries = [];
    for (let i = 1; i < sizes.length; i++)
    {
        const lo = sizes[i - 1];
        const hi = sizes[i];
        boundaries.push(Math.sqrt(lo * hi)); // Geometric mean
    }

    // console.log("BOUNDARIES", boundaries);

    return (bytes) => {
        for (let i = 0; i < boundaries.length; i++)
        {
            if (bytes < boundaries[i])
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

function getOrThrow(solution, index) {
    if (index < 0 || index >= array.index)
        throw { message: "out of range solution access",  array, index };
    return solution[index];
}

export function* complexMerges({solver, workers, maxSourceParts, insertPartSize, mergeRight})
{
    const sizes = targetSizesForBases(insertPartSize, layerBases);
    const layerFunc = layerFuncForBases(sizes);
    const mt = yield {type: 'getMergeTree'};

    const settings = {
        max_parts_to_merge_at_once: 0, // TODO(serxa): remove it or replace with maxSourceParts? how to merge to long grid intervals?
    };

    while (true)
    {
        const active_parts = mt.sortedActiveParts();

        // Sort parts by byte size ASC
        const sorted_parts = [...active_parts];
        sorted_parts.sort((a, b) => a.bytes - b.bytes);

        // Find best number of layers to use with given set of parts
        const B = mt.inserted_bytes;
        const merge_speed = 1.0 / mt.mergeDuration(1.0, 0);
        const logB = Math.log(B);
        const xMax = Math.log(maxSourceParts);
        const modelingResults = [];
        for (let L = 1; L < 10; L++) {
            // Model merging process through all layers l=1..L
            let targets = [];
            let boundaries = [];
            let integral_bytes = 0; // bytes would be required to merge everything
            let integral_part_count = 0; // part count times seconds integral

            let k = 0; // parts processed
            let sum_utility = 0; // Note `utility` uses log base 2, make sure to multiply by ln2 to convert to natural logarithm
            let sum_bytes = 0;
            for (let i = 0; i < L; i++) {
                let l = L - i;
                // TODO(serxa): do we need special case for l=1?

                // TODO(serxa): we need to compute x_opt only for parts in layer - merge them into 1 part (smaller than all parts), check for 'Math.exp(avg_entropy)'
                // Handle a set of parts that participate in merges on l-th layer of L-layered MT
                // Include all parts that satisfy condition: h[l] - 0.5 * x_opt[l] <= log B - log b[k]
                // , where h[l] = log B - sum_k(b[k] * log b[k]) / B - average entropy of parts in l-th layer
                //   B - total number of bytes, b[k] - bytes in k-th part
                //   x_opt[l] - optimal entropy of merges within l-layered MT with h[l] height and N workers
                // Note that we dont need the set of parts, only a few aggregates
                let avg_entropy;
                let opt_entropy;
                const calc_avg_entropy = () => logB - sum_utility * Math.LN2 / B;
                const calc_opt_entropy = () => Math.min(xMax, getOrThrow(solver.solve({parts: Math.exp(avg_entropy), workers}), l - 1);
                const boundary_check = (entropy) => entropy < avg_entropy - opt_entropy / 2;
                // TODO(serxa): optimize it by using prefix sums and binary search?
                for (; k < sorted_parts.length; k++) {
                    const p = sorted_parts[k];
                    if (k > 0) {
                        const entropy = logB - p.log_bytes * Math.LN2;
                        if (boundary_check(entropy)) {
                            avg_entropy = calc_avg_entropy();
                            opt_entropy = calc_opt_entropy();
                            if (boundary_check(entropy))
                                break; // This part belongs to an upper layer
                        }
                    }
                    sum_utility += p.utility;
                    sum_bytes += p.bytes;
                    if (avg_entropy === undefined) {
                        // TODO(serxa): if smallest part falls out or model range - stick to the smallest part size available in solver
                        avg_entropy = calc_avg_entropy();
                        opt_entropy = calc_opt_entropy();
                    }
                }

                // Save current l of L layers result
                const layer_entropy = Math.log(sum_bytes) - sum_utility * Math.LN2 / sum_bytes;
                const solutions = solver.solve({parts: Math.exp(layer_entropy), workers};
                const target = Math.exp(logB - avg_entropy + opt_entropy);
                target_sizes.push(target);
                boundaries.push(Math.exp(logB - avg_entropy + opt_entropy / 2));
                const duration = sum_bytes / merge_speed;
                const layer_parts = solver.F0(opt_entropy, Math.exp(avg_entropy), workers);
                const other_parts = sorted_parts.length - k;
                integral_part_count += duration * (layer_parts + other_parts);
                integral_bytes += sum_bytes;

                // Model merges onto the next layer
                sum_utility = sum_bytes * Math.log2(target);
            }
            modelingResults.push({
                L,
                target_sizes,
                write_bytes,
            });
        }

        // Choose best number of layers
        let Lopt; // TODO(serxa)

        let best_begin = -1;
        let best_end = -1;
        let best_layer = layerBases.length;
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

                // Check alignment with begining of target interval
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
