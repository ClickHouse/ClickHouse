export function* simpleMerges()
{
    const mt = yield {type: 'getMergeTree'};

    const settings =
    {
        /// Zero means unlimited. Can be overridden by the same merge tree setting.
        max_parts_to_merge_at_once: 100,
        min_parts: 2,

        /** Minimum ratio of size of one part to all parts in set of parts to merge (for usual cases).
          * For example, if all parts have equal size, it means, that at least 'base' number of parts should be merged.
          * If parts has non-uniform sizes, then minimum number of parts to merge is effectively increased.
          * This behaviour balances merge-tree workload.
          * It called 'base', because merge-tree depth could be estimated as logarithm with that base.
          *
          * If base is higher - then tree gets more wide and narrow, lowering write amplification.
          * If base is lower - then merges occurs more frequently, lowering number of parts in average.
          *
          * We need some balance between write amplification and number of parts.
          */
        base: 5,

        /** Base is lowered until 1 (effectively means "merge any two parts") depending on several variables:
          *
          * 1. Total number of parts in partition. If too many - then base is lowered.
          * It means: when too many parts - do merges more urgently.
          *
          * 2. Minimum age of parts participating in merge. If higher age - then base is lowered.
          * It means: do less wide merges only rarely.
          *
          * 3. Sum size of parts participating in merge. If higher - then more age is required to lower base. So, base is lowered slower.
          * It means: for small parts, it's worth to merge faster, even not so wide or balanced.
          *
          * We have multivariative dependency. Let it be logarithmic of size and somewhat multi-linear by other variables,
          *  between some boundary points, and constant outside.
          */

        min_size_to_lower_base: 1024 * 1024,
        max_size_to_lower_base: 100 * 1024 * 1024 * 1024,

        min_age_to_lower_base_at_min_size: 10,
        min_age_to_lower_base_at_max_size: 10,
        max_age_to_lower_base_at_min_size: 3600,
        max_age_to_lower_base_at_max_size: 30 * 86400,

        min_parts_to_lower_base: 10,
        max_parts_to_lower_base: 50,

        /// Add this to size before all calculations. It means: merging even very small parts has it's fixed cost.
        size_fixed_cost_to_add: 5 * 1024 * 1024,

        /** Heuristic:
          * Make some preference for ranges, that sum_size is like (in terms of ratio) to part previous at left.
          */
        enable_heuristic_to_align_parts: true,
        heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part: 0.9,
        heuristic_to_align_parts_max_absolute_difference_in_powers_of_two: 0.5,
        heuristic_to_align_parts_max_score_adjustment: 0.75,

        /** If it's not 0, all part ranges that have min_age larger than min_age_to_force_merge
          * will be considered for merging
          */
        min_age_to_force_merge: 0,

        /** Heuristic:
          * From right side of range, remove all parts, that size is less than specified ratio of sum_size.
          */
        enable_heuristic_to_remove_small_parts_at_right: true,
        heuristic_to_remove_small_parts_at_right_max_ratio: 0.01,
    };

    function interpolateLinear(min, max, ratio)
    {
        return min + (max - min) * ratio;
    }

    function mapPiecewiseLinearToUnit(value, min, max)
    {
        if (value <= min) {
            return 0;
        } else if (value >= max) {
            return 1;
        } else {
            return (value - min) / (max - min);
        }
    }

    function allow({
        sum_size,
        max_size,
        min_age,
        range_size,
        partition_size,
        min_size_to_lower_base_log,
        max_size_to_lower_base_log})
    {
        if (settings.min_age_to_force_merge && min_age >= settings.min_age_to_force_merge)
            return true;
        /// Map size to 0..1 using logarithmic scale
        /// Use log(1 + x) instead of log1p(x) because our sum_size is always integer.
        /// Also log1p seems to be slow and significantly affect performance of merges assignment.
        let size_normalized = mapPiecewiseLinearToUnit(Math.log(1 + sum_size), min_size_to_lower_base_log, max_size_to_lower_base_log);
        /// Calculate boundaries for age
        let min_age_to_lower_base = interpolateLinear(settings.min_age_to_lower_base_at_min_size, settings.min_age_to_lower_base_at_max_size, size_normalized);
        let max_age_to_lower_base = interpolateLinear(settings.max_age_to_lower_base_at_min_size, settings.max_age_to_lower_base_at_max_size, size_normalized);
        /// Map age to 0..1
        let age_normalized = mapPiecewiseLinearToUnit(min_age, min_age_to_lower_base, max_age_to_lower_base);
        /// Map partition_size to 0..1
        let num_parts_normalized = mapPiecewiseLinearToUnit(partition_size, settings.min_parts_to_lower_base, settings.max_parts_to_lower_base);
        let combined_ratio = Math.min(1.0, age_normalized + num_parts_normalized);
        let lowered_base = interpolateLinear(settings.base, 2.0, combined_ratio);
        return (sum_size + range_size * settings.size_fixed_cost_to_add) / (max_size + settings.size_fixed_cost_to_add) >= lowered_base;
    }

    const min_size_to_lower_base_log = Math.log(1 + settings.min_size_to_lower_base);
    const max_size_to_lower_base_log = Math.log(1 + settings.max_size_to_lower_base);

    while (mt.active_part_count >= 2)
    {
        let best_range = null;
        let best_begin = -1;
        let best_end = -1;
        let best_score = 0;
        for (const cur_range of mt.getRangesForMerge())
        {
            for (let begin = 0; begin < cur_range.length - settings.min_parts; begin++)
            {
                for (let end = begin + settings.min_parts; end < cur_range.length; end++)
                {
                    if (settings.max_parts_to_merge_at_once && end - begin > settings.max_parts_to_merge_at_once)
                        break;
                    const range = cur_range.slice(begin, end);
                    const count = end - begin;
                    const sum_bytes = range.reduce((a, d) => a + d.bytes, 0);
                    const score = sum_bytes / (count - 1.9);

                    let allowed = allow({
                        sum_size: sum_bytes,
                        max_size: range.reduce((a, d) => Math.max(a, d.bytes), 0),
                        min_age: range.reduce((a, d) => Math.min(a, mt.time - d.created), Infinity),
                        range_size: count,
                        partition_size: cur_range.length,
                        min_size_to_lower_base_log,
                        max_size_to_lower_base_log
                    });

                    if (allowed)
                    {
                        if (best_score == 0 || score < best_score)
                        {
                            best_begin = begin;
                            best_end = end;
                            best_range = cur_range;
                            best_score = score;
                        }
                    }
                }
            }
        }
        if (best_range)
            yield {type: 'merge', parts_to_merge: best_range.slice(best_begin, best_end)};
        else if (mt.merging_part_count > 0)
            yield {type: 'wait'};
        else
            return; // Simple merge selector thinks it is better not to merge
    }
}
