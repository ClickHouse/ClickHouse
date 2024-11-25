function layerFuncForBases(insertPartSize, layerBases)
{
    let sizes = [insertPartSize];
    for (const base of layerBases)
    {
        const prev = sizes[sizes.length - 1];
        sizes.push(prev * base);
    }

    let boundaries = [];
    for (let i = 1; i < sizes.length; i++)
    {
        const lo = sizes[i - 1];
        const hi = sizes[i];
        boundaries.push(Math.sqrt(lo * hi)); // Geometric mean
    }

    console.log("BOUNDARIES", boundaries);

    return (size) => {
        for (let i = 0; i < boundaries.length; i++)
        {
            if (size < boundaries[i])
                return i;
        }
        return boundaries.length;
    }
}

// Assigns merges based on size of source parts by "layers"
// Part layer is determined by its size with `layerFunc(size)`:
//   layer 0: smallest parts
//   layer k: parts of prev layer size multiplied by layerBases[k - 1]
//   layer L: max size parts
// Never merges parts of different layers.
// Number of parts to merge on layer is determined by `bases[0..L]` array.
// Lower layers get absolute priority over higher layers.
export function* integerLayerMerges({insertPartSize, layerBases})
{
    const layerFunc = layerFuncForBases(insertPartSize, layerBases);
    const mt = yield {type: 'getMergeTree'};

    const settings = {
        max_parts_to_merge_at_once: 100,
        min_parts: 2,
    };

    while (true)
    {
        let best_range = null;
        let best_begin = -1;
        let best_end = -1;
        let best_layer = Infinity;
        for (const cur_range of mt.getRangesForMerge())
        {
            for (let begin = 0; begin < cur_range.length - settings.min_parts; begin++)
            {
                const layer = layerFunc(cur_range[begin].bytes);
                if (layer >= best_layer)
                    continue; // We already have parts to merge on this layer
                const base = layerBases[layer];
                const end = begin + base;
                if (end > cur_range.length)
                    continue; // not enough parts to merge
                if (settings.max_parts_to_merge_at_once && base > settings.max_parts_to_merge_at_once)
                    throw { message: "Base suggest to merge more than allowed in settings", base };

                // Check that all parts have the same layer
                let allowed = true;
                for (let i = begin + 1; i < end; i++)
                {
                    if (layerFunc(cur_range[i].bytes) != layer)
                    {
                        allowed = false;
                        break;
                    }
                }

                if (allowed)
                {
                    best_begin = begin;
                    best_end = end;
                    best_range = cur_range;
                    best_layer = layer;
                    if (layer == 0)
                        break; // shortcut
                }
            }
            if (best_layer == 0)
                break; // shortcut
        }
        if (best_range)
            yield {type: 'merge', parts_to_merge: best_range.slice(best_begin, best_end)};
        else
            yield {type: 'wait'};
    }
}
