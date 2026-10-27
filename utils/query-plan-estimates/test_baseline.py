#!/usr/bin/env python3
"""Tests of the eligibility rules and the gate of `baseline.py` on small processor graphs.

    python3 -m unittest utils/query-plan-estimates/test_baseline.py
"""

import argparse
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import baseline  # noqa: E402

QUERY = 'q'


def processor(processor_id, name, step, consumers=(), output_rows=0, input_rows=0, task=QUERY, kind=None, estimate=10.0):
    return dict(query_id=task, id=processor_id, parent_ids=list(consumers), name=name, step_uniq_id=step, plan_step=hash(step),
                plan_step_name=kind or step.split('_')[0], plan_step_description='', plan_step_estimated_rows=estimate,
                plan_step_estimate_source='statistics', input_rows=input_rows, output_rows=output_rows)


def statuses(processors):
    return {r['step']: r['status'] for r in baseline.step_records(processors, QUERY)}


class Eligibility(unittest.TestCase):
    def test_read_to_the_sink_is_observed(self):
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10),
                 processor(2, 'ExpressionTransform', 'Expression_1', [3], 10, 10),
                 processor(3, 'LazyOutputFormat', '', [], 0, 10)]
        self.assertEqual(statuses(graph), {'ReadFromMergeTree_0': 'observed', 'Expression_1': 'observed'})

    def test_limit_cuts_short(self):
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10),
                 processor(2, 'Limit', 'Limit_1', [3], 5, 10),
                 processor(3, 'LazyOutputFormat', '', [], 0, 5)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'cut_short')

    def test_full_consumer_proves_completion_only_with_output(self):
        def graph(sorted_rows):
            return [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10),
                    processor(2, 'MergeSortingTransform', 'Sorting_1', [3], sorted_rows, 10),
                    processor(3, 'Limit', 'Limit_2', [4], min(sorted_rows, 5), sorted_rows),
                    processor(4, 'LazyOutputFormat', '', [], 0, 5)]
        self.assertEqual(statuses(graph(10))['ReadFromMergeTree_0'], 'observed')
        # A sort whose output was closed before it produced any may not have read all of its input.
        self.assertEqual(statuses(graph(0))['ReadFromMergeTree_0'], 'cut_short')

    def test_full_consumer_stream_without_output_is_complete(self):
        # Only the last aggregating stream to finish produces the result of the step.
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [3], 10),
                 processor(2, 'MergeTreeSelect', 'ReadFromMergeTree_0', [4], 10),
                 processor(3, 'AggregatingTransform', 'Aggregating_1', [5], 0, 10),
                 processor(4, 'AggregatingTransform', 'Aggregating_1', [5], 3, 10),
                 processor(5, 'Limit', 'Limit_2', [6], 2, 3),
                 processor(6, 'LazyOutputFormat', '', [], 0, 2)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'observed')

    def test_sort_stream_without_output_is_complete_under_a_producing_merge(self):
        # A sort stream that got no rows; the merge of the sort streams starts only after each stream finished.
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [3], 0),
                 processor(2, 'MergeTreeSelect', 'ReadFromMergeTree_0', [4], 10),
                 processor(3, 'MergeSortingTransform', 'Sorting_1', [5], 0, 0),
                 processor(4, 'MergeSortingTransform', 'Sorting_1', [5], 10, 10),
                 processor(5, 'MergingSortedTransform', 'Sorting_1', [6], 2, 10),
                 processor(6, 'Limit', 'Limit_2', [7], 2, 2),
                 processor(7, 'LazyOutputFormat', '', [], 0, 2)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'observed')

    def test_sort_streams_of_separate_partitions_prove_only_their_own_completion(self):
        # The streams of a sort of separate partitions are not merged: the second one stopped before its first chunk.
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [3], 10),
                 processor(2, 'MergeTreeSelect', 'ReadFromMergeTree_0', [4], 100),
                 processor(3, 'MergeSortingTransform', 'Sorting_1', [5], 10, 10),
                 processor(4, 'MergeSortingTransform', 'Sorting_1', [5], 0, 100),
                 processor(5, 'Limit', 'Limit_2', [6], 1, 10),
                 processor(6, 'LazyOutputFormat', '', [], 0, 1)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'cut_short')

    def test_empty_filter_cuts_short(self):
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10),
                 processor(2, 'FilterTransform', 'Filter_1', [3], 0, 10),
                 processor(3, 'LazyOutputFormat', '', [], 0, 0)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'cut_short')

    def test_one_empty_filter_stream_is_complete(self):
        # One of two streams of a filter step passes no rows; the step as a whole produced rows.
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10),
                 processor(5, 'MergeTreeSelect', 'ReadFromMergeTree_0', [6], 10),
                 processor(2, 'FilterTransform', 'Filter_1', [3], 0, 10),
                 processor(6, 'FilterTransform', 'Filter_1', [3], 4, 10),
                 processor(3, 'LazyOutputFormat', '', [], 0, 4)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'observed')

    def test_unknown_processor_is_unsupported(self):
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10),
                 processor(2, 'SomeNewTransform', 'Expression_1', [3], 10, 10),
                 processor(3, 'LazyOutputFormat', '', [], 0, 10)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'unsupported_shape')

    def test_reused_processor_id_is_ambiguous(self):
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10),
                 processor(2, 'ExpressionTransform', 'Expression_1', [3], 10, 10),
                 processor(2, 'ExpressionTransform', 'Expression_5', [3], 7, 7),
                 processor(3, 'LazyOutputFormat', '', [], 0, 10)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'ambiguous')

    def test_second_output_is_mixed_unless_it_splits_one_result(self):
        def graph(name):
            return [processor(1, name, 'Aggregating_0', [2, 3], 10),
                    processor(2, 'ExpressionTransform', 'Expression_1', [4], 5, 5),
                    processor(3, 'ExpressionTransform', 'Expression_1', [4], 5, 5),
                    processor(4, 'LazyOutputFormat', '', [], 0, 10)]
        self.assertEqual(statuses(graph('CopyTransform'))['Aggregating_0'], 'mixed_outputs')
        self.assertEqual(statuses(graph('Resize'))['Aggregating_0'], 'observed')

    def test_tasks_beyond_receiving_the_result_are_distributed(self):
        graph = [processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_0', [2], 10, task='q::stage_1'),
                 processor(2, 'SinkFromInMemoryExchange', 'GatherSend_1', [], 0, 10, task='q::stage_1'),
                 processor(1, 'MergeTreeSelect', 'ReadFromMergeTree_2', [2], 10, task='q::stage_2'),
                 processor(2, 'SinkFromInMemoryExchange', 'GatherSend_3', [], 0, 10, task='q::stage_2'),
                 processor(3, 'SourceFromInMemoryExchange', 'ReadFromPreparedSource_4', [4], 20),
                 processor(4, 'LazyOutputFormat', '', [], 0, 20)]
        self.assertEqual(statuses(graph)['ReadFromMergeTree_0'], 'distributed')

    def test_totals_streams_make_the_execution_ineligible(self):
        # An expression above `WITH TOTALS` has one processor for the main stream and one for the totals stream.
        graph = [processor(1, 'TotalsHavingTransform', 'TotalsHaving_0', [2, 3], 11),
                 processor(2, 'ExpressionTransform', 'Expression_1', [4], 10, 10),
                 processor(3, 'ExpressionTransform', 'Expression_1', [4], 1, 1),
                 processor(4, 'LazyOutputFormat', '', [], 0, 11)]
        self.assertEqual(statuses(graph)['Expression_1'], 'auxiliary_streams')

    def test_remote_source_is_distributed(self):
        graph = [processor(1, 'Remote', 'ReadFromRemote_0', [2], 10),
                 processor(2, 'LazyOutputFormat', '', [], 0, 10)]
        self.assertEqual(statuses(graph)['ReadFromRemote_0'], 'distributed')


class Settings(unittest.TestCase):
    reference = {'enable_join_runtime_filters': '0'}
    compiled = {'enable_join_runtime_filters': '1'}

    def test_changed_setting_matches(self):
        logged = {'enable_join_runtime_filters': '0', 'log_comment': 'c'}
        self.assertIsNone(baseline.ineligibility(logged, self.compiled, self.reference, ['db.t'], 'db'))

    def test_reset_to_default_is_the_compiled_default(self):
        # `SETTINGS enable_join_runtime_filters = DEFAULT` leaves the setting out of the log, and it is then on.
        logged = {'log_comment': 'c'}
        self.assertIn('settings differ', baseline.ineligibility(logged, self.compiled, self.reference, ['db.t'], 'db'))

    def test_table_outside_the_database(self):
        logged = {'enable_join_runtime_filters': '0', 'log_comment': 'c'}
        problem = baseline.ineligibility(logged, self.compiled, self.reference, ['db.v', 'other.t'], 'db')
        self.assertIn('outside', problem)


class Statements(unittest.TestCase):
    def test_one_plain_statement(self):
        self.assertIsNone(baseline.unsupported_statement(['SELECT sum(x) FROM t']))

    def test_inner_settings_are_unsupported(self):
        inner = 'SELECT sum(x) FROM (SELECT x FROM t SETTINGS max_rows_to_read = 10, read_overflow_mode = \'break\')'
        self.assertIsNotNone(baseline.unsupported_statement([inner]))

    def test_several_statements_are_unsupported(self):
        self.assertIsNotNone(baseline.unsupported_statement(['CREATE VIEW v AS SELECT 1', 'SELECT * FROM v']))

    def test_having_prefilter_is_ineligible(self):
        # The `HAVING` bound inside an aggregation drops groups, so its output is not the groups its estimate counts.
        compiled = {'query_plan_aggregation_having_prefilter': '1'}
        problem = baseline.ineligibility({'log_comment': 'c'}, compiled, baseline.REFERENCE_SETTINGS, ['db.t'], 'db')
        self.assertIn('query_plan_aggregation_having_prefilter', problem)

    def test_user_defined_function_input_is_ineligible(self):
        problem = baseline.ineligibility({'log_comment': 'c'}, {}, {}, ['db.t'], 'db', ['eligible'])
        self.assertIn('fingerprint does not cover', problem)

    def test_query_parameter_is_unsupported(self):
        self.assertIsNotNone(baseline.unsupported_statement(['SELECT sum(x) FROM t WHERE id < {cutoff:UInt64}']))
        self.assertIsNotNone(baseline.unsupported_statement(['SELECT sum(x) FROM t WHERE id < {$cutoff:UInt64}']))

    def test_volatile_function_is_ineligible(self):
        problem = baseline.ineligibility({'log_comment': 'c'}, {}, {}, ['db.t'], 'db', [], ['today'])
        self.assertIn('differ between runs', problem)

    def test_dictionary_input_is_ineligible(self):
        logged = {'log_comment': 'c'}
        problem = baseline.ineligibility(logged, {}, {}, ['db.t'], 'db', ['other_db.d'])
        self.assertIn('fingerprint does not cover', problem)


def manifest(collection='ok', changed=None, runs=1):
    executions = [dict(run=run, query_id='x%d' % run, outcome='success', collection=collection,
                       changed_settings=(changed or {}).get(run, {'enable_cascades_optimizer': '1'}))
                  for run in range(1, runs + 1)]
    return dict(schema_version=baseline.SCHEMA_VERSION, run_id='r', label='l', database='db', data_fingerprint='f',
                compiled_default_settings={'enable_cascades_optimizer': '0'},
                statements=[dict(query=QUERY, sha256='s', executions=executions)])


def record(step, estimated, actual, status='observed'):
    return dict(step=step, kind='Join', description='', estimated=estimated, actual=actual, source='statistics',
                status=status, query=QUERY, run=1)


class Gate(unittest.TestCase):
    def compare(self, old_records, new_records, old=None, new=None):
        def runs(records):
            return {(QUERY, r['step']): {1: r} for r in records}
        old_manifest = old or manifest()
        new_manifest = new or manifest()
        reasons = baseline.comparability(old_manifest, new_manifest)
        if reasons:
            return 'not comparable'
        args = argparse.Namespace(tolerance=0.05)
        return baseline.compare_kind('Join', baseline.by_query(old_manifest, runs(old_records)),
                                     baseline.by_query(new_manifest, runs(new_records)), args.tolerance)[0]

    def test_lost_observation_fails(self):
        many = [record('Join_%d' % i, 10, 10) for i in range(12)]
        lost = many[:-1] + [record('Join_11', 10, 10, status='unstable')]
        self.assertEqual(self.compare(many, many), 'pass')
        self.assertEqual(self.compare(many, lost), 'fail')

    def test_changed_plan_with_fewer_observations_fails(self):
        before = [record('Join_%d' % i, 10, 10) for i in range(12)]
        after = [record('Join_%d' % (i + 100), 10, 10) for i in range(11)]
        self.assertEqual(self.compare(before, after), 'fail')

    def test_new_one_zero_pair_fails(self):
        before = [record('Join_%d' % i, 10, 10) for i in range(12)]
        after = before[:-1] + [record('Join_11', 10, 0)]
        self.assertEqual(self.compare(before, after), 'fail')

    def test_too_few_pairs_are_insufficient(self):
        few = [record('Join_%d' % i, 10, 10) for i in range(3)]
        self.assertEqual(self.compare(few, few), 'insufficient evidence')

    def test_different_effective_settings_are_not_comparable(self):
        other = manifest(changed={1: {'enable_cascades_optimizer': '0'}})
        same = [record('Join_%d' % i, 10, 10) for i in range(12)]
        self.assertEqual(self.compare(same, same, new=other), 'not comparable')
        # The compared model settings may differ.
        models = manifest(changed={1: {'enable_cascades_optimizer': '1', 'query_plan_join_multi_key_model': 'postgres'}})
        self.assertEqual(self.compare(same, same, new=models), 'pass')

    def test_every_repeat_is_compared(self):
        old = manifest(runs=2)
        new = manifest(runs=2, changed={1: {'enable_cascades_optimizer': '0'}, 2: {'enable_cascades_optimizer': '1'}})
        self.assertIn('different effective settings', baseline.comparability(old, new))


if __name__ == '__main__':
    unittest.main()
