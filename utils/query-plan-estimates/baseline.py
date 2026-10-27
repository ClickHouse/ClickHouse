#!/usr/bin/env python3
"""Run a benchmark suite on a local server, record the estimated and actual rows of every query plan step, and gate
one run against another.

    baseline.py run --queries DIR --database DB --label NAME --out DIR [--settings FILE.json] [--runs N] [--client CMD]
    baseline.py report DIR
    baseline.py compare OLD_DIR NEW_DIR [--tolerance T]

`run` executes every `*.sql` file of DIR (sorted by name) N times with the reference settings below pinned, and
writes to an empty output directory:
  - `manifest.json`: schema version, run id, label, server version, build id and git hash, the source commit if
    given, the settings, and per statement file its digest and, per execution, the query id, the outcome (`pending`,
    `success`, `execution_failure` with the error code; a file with several statements is `unsupported` and not run)
    and whether collecting its observations worked. It is written before the first execution and after each, so an
    interrupted run shows which executions are unknown.
  - `observations.jsonl`: one record per plan step of each successful execution.

A step's actual rows are the rows its processors pass to other steps (as `step_estimate_vs_actual.sql` counts them).
A record enters the accuracy report only with status `observed`, which needs all of these:
  - the effective settings of the execution (`system.query_log`) have the reference values: runtime filters off (so
    the actual rows are those of the logical result the estimate describes), the join order history off, no size
    limits that stop an operator early, no query cache; otherwise `settings`;
  - the execution ran in one task, or in one task whose result the initiator only receives (a distributed plan
    executed locally with one bucket per stage); otherwise `distributed`;
  - the processor graph of the task is complete and unambiguous: every processor id is logged once and every consumer
    it names is logged. Processor ids are addresses, so the pipelines of a scalar subquery and of the main query can
    reuse them; otherwise `ambiguous`;
  - every counted processor of the step has one connected output, or several that split one result (`Resize`):
    `output_rows` counts all ports together, so a second output such as totals or extremes would be added in;
    otherwise `mixed_outputs` or `terminal`;
  - the step's output was read to the end: every path from it to the task's sink passes only processors that stream
    their input and stop reading only when their own output is closed, until a processor that reads all of its input
    or the sink. A processor that can stop on its own (`Limit`, `DISTINCT`, merging of sorted streams, sets, a join
    whose build side is empty) makes it `cut_short`; a processor the tool does not know, `unsupported_shape`.
Every other record keeps its status and stays in the counts, so the report shows how much of the plan it could not
judge.
"""

import argparse
import hashlib
import re
import json
import math
import os
import shlex
import subprocess
import sys
import uuid
from collections import defaultdict

SCHEMA_VERSION = 1

# Settings every measured execution has, so that the output of each step is the complete logical result.
REFERENCE_SETTINGS = {
    'enable_join_runtime_filters': '0',
    'use_hash_table_stats_for_join_reordering': '0',
    'max_rows_in_join': '0',
    'max_bytes_in_join': '0',
    'max_rows_to_group_by': '0',
    'max_rows_in_set': '0',
    'max_bytes_in_set': '0',
    'max_rows_in_distinct': '0',
    'max_bytes_in_distinct': '0',
    # A sort that spills counts its writes to the spill file as output before it has read all of its input.
    'max_bytes_before_external_sort': '0',
    'max_bytes_ratio_before_external_sort': '0',
    'use_query_cache': '0',
    # Local execution: no parallel replicas, and a distributed plan runs in this process.
    'allow_experimental_parallel_reading_from_replicas': '0',
    'distributed_plan_execute_locally': '1',
    'log_processors_profiles': '1',
    'log_query_settings': '1',
    # Compatibility changes the defaults the effective settings are reconstructed from.
    'compatibility': '',
    # Optimizations that drop rows inside an operator that reads all of its input, so that its output is fewer rows
    # than the logical result its estimate describes: the `HAVING` bound inside an aggregation and the top-k paths.
    'query_plan_aggregation_having_prefilter': '0',
    'query_plan_aggregation_bucket_top_k': '0',
    'enable_group_by_top_k_optimization': '0',
    'enable_group_by_top_k_dynamic_filtering': '0',
    'use_top_k_dynamic_filtering': '0',
    'use_skip_indexes_for_top_k': '0',
    'use_query_condition_cache_for_top_k': '0',
    'query_plan_top_k_through_join': '0',
    'query_plan_max_limit_for_top_k_optimization': '0',
}

# Processors that read all of their input before they produce output, under the reference settings. Any of them
# stops reading when its output is closed before it produced any (a `Limit` above a `Resize` that another branch also
# feeds), so only `TaskGraph.read_to_end` proves that its input was read to the end.
FULL_CONSUMERS = {'MergeSortingTransform', 'AggregatingTransform', 'FillingRightJoinSide'}
# Processors that stream their input and stop reading it only when their own output is closed.
PASS_THROUGH = {
    'ExpressionTransform', 'FilterTransform', 'SimpleSquashingTransform', 'Resize', 'StrictResize',
    'ColumnPermuteTransform', 'LimitsCheckingTransform', 'ScatterByPartitionTransform', 'Concat',
    'PartialSortingTransform', 'DelayedJoinedBlocksTransform', 'DelayedJoinedBlocksWorkerTransform', 'DelayedPorts',
    'NonJoinedBlocksTransform', 'WindowTransform', 'RollupTransform', 'GroupingAggregatedTransform',
    'MergingAggregatedBucketTransform', 'JoiningTransform',
}
# Pass-through processors that stop reading on their own when they produce nothing: a filter that becomes constantly
# false, a join that is provably empty or has an empty build side. They then produce nothing on any stream of their
# step, so the test is on the step's total: one stream without output is normal.
STOP_WHEN_EMPTY = {'FilterTransform', 'JoiningTransform'}
# Processors that can stop reading their input on their own.
STOPPERS = {
    'Limit', 'LimitRange', 'Offset', 'DistinctTransform', 'DistinctSortedTransform', 'DistinctSortedStreamTransform',
    'ExternalDistinctTransform', 'MergingSortedTransform', 'CreatingSetsTransform', 'IntersectOrExcept',
}
# The inputs of a query besides tables, as `system.query_log` records them.
OTHER_INPUTS = ['used_dictionaries', 'used_table_functions', 'used_sql_user_defined_functions',
                'used_executable_user_defined_functions', 'used_row_policies']
# Processors that add auxiliary result streams (totals, extremes): the steps after them get processors for those streams
# too, which a count of the step's output would add to the main result.
AUXILIARY_STREAMS = {'TotalsHavingTransform', 'ExtremesTransform'}
# Processors that read from or send to another server.
REMOTE = {'Remote', 'RemoteTotals', 'RemoteExtremes', 'RemoteSink', 'ReadFromParallelReplicas'}
# Processors whose every output carries rows of the same result, so `output_rows` over all of them is that result.
SPLITTING = {'Resize', 'StrictResize', 'ScatterByPartitionTransform'}
# Processors that end the output of a task.
SINKS = {'LazyOutputFormat', 'EmptySink', 'SinkFromInMemoryExchange', 'NullSink'}
# Steps of the initiator task that only receive the result of a distributed plan.
RECEIVE_STEPS = {'', 'ReadFromPreparedSource'}
MIN_PAIRS = 10


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('run')
    run.add_argument('--queries', required=True)
    run.add_argument('--database', required=True)
    run.add_argument('--label', required=True)
    run.add_argument('--out', required=True)
    run.add_argument('--settings', help='a JSON file with an object "settings"')
    run.add_argument('--runs', type=int, default=2)
    run.add_argument('--client', default='clickhouse client', help='the client command, split as a shell would')
    run.add_argument('--source-commit')
    report = commands.add_parser('report')
    report.add_argument('directory')
    compare = commands.add_parser('compare')
    compare.add_argument('old')
    compare.add_argument('new')
    compare.add_argument('--tolerance', type=float, default=0.05,
                         help='the growth of the geometric mean q-error that fails the gate. A policy margin, not a '
                              'noise estimate: the estimates do not change between runs, and `report` counts the steps '
                              'whose rows do as `unstable`')
    args = parser.parse_args()
    if args.command == 'run' and args.runs < 1:
        parser.error('--runs must be positive')
    return args


class Client:
    def __init__(self, command, database):
        self.command = shlex.split(command) + ['--database', database]
        program = self.command[0]
        # `clickhouse client` comes with `clickhouse format`, which splits a file into statements.
        if len(self.command) > 1 and self.command[1] == 'client':
            self.format_command = [program, 'format']
        else:
            self.format_command = [program.replace('-client', '-format')]

    def execute(self, query, extra=()):
        result = subprocess.run(self.command + list(extra) + ['--query', query],
                                capture_output=True, text=True, stdin=subprocess.DEVNULL)
        return result.returncode, result.stdout, result.stderr

    def fetch(self, query, **parameters):
        extra = ['--param_%s=%s' % (name, value) for name, value in parameters.items()]
        code, out, err = self.execute(query + ' FORMAT JSONEachRow', extra)
        if code != 0:
            raise RuntimeError(err.strip()[-500:])
        return [json.loads(line) for line in out.splitlines() if line]

    def statements(self, text):
        """The statements of a file, one formatted line each."""
        result = subprocess.run(self.format_command + ['--multiquery', '--oneline'], input=text,
                                capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError('cannot split into statements: ' + result.stderr.strip()[-300:])
        return [line for line in result.stdout.splitlines() if line.strip()]


def error_code(stderr):
    marker = 'Code: '
    position = stderr.find(marker)
    if position < 0:
        return None
    digits = stderr[position + len(marker):].split('.', 1)[0]
    return int(digits) if digits.isdigit() else None


def is_local(processors, query_id):
    """One task, or one task whose result the initiator only receives, and no processor that talks to another server.
    With the reference settings a distributed plan runs in this process, so its tasks log here."""
    if any(p['name'] in REMOTE for p in processors):
        return False
    tasks = defaultdict(set)
    for processor in processors:
        tasks[processor['query_id']].add(processor['plan_step_name'])
    return len(tasks) == 1 or (len(tasks) == 2 and query_id in tasks and tasks[query_id] <= RECEIVE_STEPS)


def complete_tasks(processors):
    """The tasks whose processor graph is complete and unambiguous."""
    rows = defaultdict(int)
    for processor in processors:
        rows[(processor['query_id'], processor['id'])] += 1
    incomplete = {task for (task, _), count in rows.items() if count > 1}
    for processor in processors:
        task = processor['query_id']
        if any((task, consumer) not in rows for consumer in processor['parent_ids']):
            incomplete.add(task)
    return {processor['query_id'] for processor in processors} - incomplete


class TaskGraph:
    """The processors of the complete tasks of one execution, and how the output of a processor is consumed."""

    def __init__(self, processors):
        self.complete = complete_tasks(processors)
        self.by_key = {(p['query_id'], p['id']): p for p in processors if p['query_id'] in self.complete}
        probe_rows = defaultdict(int)
        output_rows = defaultdict(int)
        step_output_rows = defaultdict(int)
        for processor in processors:
            if processor['name'] == 'JoiningTransform':
                probe_rows[processor['step_uniq_id']] += processor['input_rows']
            if processor['name'] in STOP_WHEN_EMPTY:
                output_rows[(processor['step_uniq_id'], processor['name'])] += processor['output_rows']
        for processor in self.by_key.values():
            if all(c['step_uniq_id'] != processor['step_uniq_id'] for c in self.consumers(processor)):
                step_output_rows[processor['step_uniq_id']] += processor['output_rows']
        self.probing_steps = {step for step, rows in probe_rows.items() if rows > 0}
        self.empty_steps = {key for key, rows in output_rows.items() if rows == 0}
        self.producing_aggregations = {step for step, rows in step_output_rows.items() if rows > 0}

    def read_to_end(self, consumer):
        """Whether a full consumer is known to have read all of its input: it produced output, or its step produced
        output only after all of its streams finished reading. The last aggregating stream to finish merges the states
        of all streams of its step, so one aggregating stream without output is normal. A sort stream can get no rows;
        the merge of the sort streams starts only when each one has sent its first chunk or finished. A join build is
        read to the end when its join started probing. A sort without sort keys would pass chunks through instead; a
        query does not have one, as the planner drops an `ORDER BY` of constants, and the tool takes that as given."""
        if consumer['name'] == 'FillingRightJoinSide':
            return consumer['step_uniq_id'] in self.probing_steps
        if consumer['output_rows'] > 0:
            return True
        if consumer['name'] == 'AggregatingTransform':
            return consumer['step_uniq_id'] in self.producing_aggregations
        return self.feeds_producing_merge(consumer)

    def feeds_producing_merge(self, processor):
        """Whether `processor` feeds a merge of sorted streams of its own step that produced output. Sort streams of
        separate partitions have no merge, and each of them proves only its own completion."""
        for consumer in self.consumers(processor):
            if consumer['step_uniq_id'] != processor['step_uniq_id']:
                continue
            if consumer['name'] == 'MergingSortedTransform' and consumer['output_rows'] > 0:
                return True
            if self.feeds_producing_merge(consumer):
                return True
        return False

    def consumers(self, processor):
        return [self.by_key[(processor['query_id'], c)] for c in processor['parent_ids']]

    def consumption(self, processor, seen):
        """`complete`, `cut_short` or `unsupported_shape` for the output of `processor`."""
        for consumer in self.consumers(processor):
            key = (consumer['query_id'], consumer['id'])
            if key in seen:
                continue
            seen.add(key)
            name = consumer['name']
            if name in SINKS or (name in FULL_CONSUMERS and self.read_to_end(consumer)):
                continue
            if name in STOPPERS or (consumer['step_uniq_id'], name) in self.empty_steps:
                return 'cut_short'
            if name not in PASS_THROUGH and name not in FULL_CONSUMERS:
                return 'unsupported_shape'
            below = self.consumption(consumer, seen)
            if below != 'complete':
                return below
        return 'complete'


def step_status(graph, step_id, members, local):
    """The status of a step and, when it is countable, its actual rows."""
    if not local:
        return 'distributed', None
    if any(p['query_id'] not in graph.complete for p in members) or len({p['plan_step'] for p in members}) > 1:
        return 'ambiguous', None
    rows = 0
    for processor in members:
        inside = [c for c in graph.consumers(processor) if c['step_uniq_id'] == step_id]
        if inside:
            if len(inside) != len(processor['parent_ids']):
                return 'mixed_outputs', None
            continue
        if not processor['parent_ids']:
            return 'terminal', None
        if len(processor['parent_ids']) > 1 and processor['name'] not in SPLITTING:
            return 'mixed_outputs', None
        consumed = graph.consumption(processor, set())
        if consumed != 'complete':
            return consumed, None
        rows += processor['output_rows']
    return 'observed', rows


def step_records(processors, query_id):
    """The plan steps of one execution from the processor rows of all its tasks."""
    graph = TaskGraph(processors)
    local = is_local(processors, query_id)
    auxiliary = any(p['name'] in AUXILIARY_STREAMS for p in processors)
    steps = defaultdict(list)
    for processor in processors:
        steps[processor['step_uniq_id']].append(processor)
    records = []
    for step_id, members in sorted(steps.items()):
        first = members[0]
        if not first['plan_step_name']:
            continue
        status, rows = ('auxiliary_streams', None) if auxiliary else step_status(graph, step_id, members, local)
        estimate = first['plan_step_estimated_rows']
        if status == 'observed' and estimate is None:
            status = 'no_estimate'
        records.append(dict(step=step_id, kind=first['plan_step_name'], description=first['plan_step_description'],
                            estimated=estimate, source=first['plan_step_estimate_source'], actual=rows, status=status))
    return records


def write_json(path, value):
    with open(path + '.tmp', 'w') as f:
        json.dump(value, f, indent=1, sort_keys=True)
    os.replace(path + '.tmp', path)


def unsupported_statement(statements):
    """Why a file cannot be measured, or None: several statements (the suite, not the measured file, does setup and
    cleanup), or a `SETTINGS` clause, whose settings apply to its subquery only and are not in the query log."""
    if len(statements) != 1:
        return 'several statements'
    if ' SETTINGS ' in ' %s ' % statements[0]:
        return 'a SETTINGS clause, which the query log does not show'
    # A query parameter takes its value from the client, which the statement digest does not cover.
    if re.search(r'\{[\w$]+\s*:', statements[0]):
        return 'a query parameter, whose value the statement digest does not cover'
    return None


def ineligibility(logged, compiled_defaults, reference, tables, database, other_inputs=(), volatile_functions=()):
    """Why no step of an execution can be observed, or None: its effective settings (the compiled defaults with the
    changes the query log records; a setting reset to `DEFAULT` is not recorded and is its compiled default) differ
    from the reference, or it read a table outside the database the fingerprint covers."""
    effective = dict(compiled_defaults, **logged)
    mismatches = sorted(name for name, value in reference.items() if effective.get(name) != value)
    if mismatches:
        return 'settings differ from the reference: ' + ', '.join(mismatches)
    outside = sorted(table for table in tables if not table.startswith(database + '.'))
    if outside:
        return 'reads tables outside the fingerprinted database: ' + ', '.join(outside)
    if other_inputs:
        return 'reads inputs the fingerprint does not cover: ' + ', '.join(sorted(other_inputs))
    if volatile_functions:
        return 'uses functions whose result can differ between runs: ' + ', '.join(sorted(volatile_functions))
    return None


def collect(client, statement, execution, reference, manifest, out):
    """Writes the observations of one execution and returns how collecting them went."""
    try:
        finishes = client.fetch("SELECT query_duration_ms, Settings, tables, used_functions, %s FROM system.query_log "
                                "WHERE query_id = {id:String} AND type = 'QueryFinish'" % ', '.join(OTHER_INPUTS),
                                id=execution['query_id'])
        processors = client.fetch('SELECT query_id, id, parent_ids, name, step_uniq_id, plan_step, plan_step_name, '
                                  'plan_step_description, plan_step_estimated_rows, plan_step_estimate_source, '
                                  'input_rows, output_rows FROM system.processors_profile_log '
                                  'WHERE initial_query_id = {id:String}', id=execution['query_id'])
    except RuntimeError as e:
        return 'failed: ' + str(e)
    if len(finishes) != 1:
        return 'failed: %d finish records in system.query_log' % len(finishes)
    if not processors:
        return 'failed: no processors logged'
    execution['server_ms'] = int(finishes[0]['query_duration_ms'])
    # `Settings` of the log has the settings that are changed, when settings logging is on. `log_comment` is always
    # changed, so without it the log has no evidence of the settings.
    logged = finishes[0]['Settings']
    if 'log_comment' not in logged:
        return 'failed: the query log has no settings of the execution'
    execution['changed_settings'] = {name: value for name, value in sorted(logged.items()) if name != 'log_comment'}
    execution['tables'] = sorted(finishes[0]['tables'])
    # Dictionaries, table functions, user-defined functions and row policies are not in `tables`, and the fingerprint
    # does not cover them.
    other_inputs = sum((finishes[0][column] for column in OTHER_INPUTS), [])
    # Functions that read the time, the user or other context, or are random, can select other rows in another run.
    # Function names are case-insensitive for many functions, so compare them in lowercase.
    volatile_names = {name.lower() for name in manifest['volatile_functions']}
    volatile = [function for function in finishes[0]['used_functions'] if function.lower() in volatile_names]
    problem = ineligibility(logged, manifest['compiled_default_settings'], reference, execution['tables'],
                            manifest['database'], other_inputs, volatile)
    for record in step_records(processors, execution['query_id']):
        if problem:
            record['status'] = 'ineligible_execution'
        record.update(query=statement['query'], run=execution['run'])
        out.write(json.dumps(record, sort_keys=True) + '\n')
    return 'ok' if not problem else 'ineligible: ' + problem


def reference_settings(client):
    """`REFERENCE_SETTINGS` and every overflow mode set to `throw`, so that no limit stops reading quietly."""
    reference = dict(REFERENCE_SETTINGS)
    for row in client.fetch("SELECT name FROM system.settings WHERE name LIKE '%overflow_mode%' AND alias_for = ''"):
        reference[row['name']] = 'throw'
    return reference


def data_fingerprint(client, database):
    """The definitions of the tables of the database and the file hashes of their active parts. Only `MergeTree`
    tables and views without `SETTINGS` qualify, because only their data and definitions the fingerprint covers."""
    tables = client.fetch('SELECT name, engine, create_table_query FROM system.tables WHERE database = {db:String} '
                          'ORDER BY name', db=database)
    # A view with a `SETTINGS` clause runs with settings that the query log does not show.
    unsupported = [t['name'] for t in tables if not t['engine'].endswith('MergeTree')
                   and (t['engine'] != 'View' or ' SETTINGS ' in t['create_table_query'])]
    if unsupported:
        sys.exit('the fingerprint does not cover the data of: ' + ', '.join(unsupported))
    parts = client.fetch('SELECT table, name, hash_of_all_files FROM system.parts WHERE database = {db:String} AND active '
                         'ORDER BY table, name', db=database)
    return hashlib.sha256(json.dumps([tables, parts], sort_keys=True).encode()).hexdigest()


def command_run(args):
    if os.path.exists(args.out) and os.listdir(args.out):
        sys.exit('the output directory %s is not empty' % args.out)
    os.makedirs(args.out, exist_ok=True)
    client = Client(args.client, args.database)
    # The query log names settings by their canonical names, not by aliases.
    aliases = {row['name']: row['alias_for'] for row in client.fetch(
        "SELECT name, alias_for FROM system.settings WHERE alias_for != ''")}
    settings = {}
    if args.settings:
        with open(args.settings) as f:
            settings = {aliases.get(name, name): value for name, value in json.load(f)['settings'].items()}
    reference = reference_settings(client)
    conflicts = sorted(n for n, v in reference.items() if n in settings and str(settings[n]) != v)
    if conflicts:
        sys.exit('the settings file overrides reference settings: ' + ', '.join(conflicts))
    settings.update(reference)
    settings_args = ['--%s=%s' % (name, value) for name, value in sorted(settings.items())]

    server = client.fetch("SELECT version() AS version, buildId() AS build_id, "
                          "(SELECT value FROM system.build_options WHERE name = 'GIT_HASH') AS git_hash")[0]
    # The compiled defaults, which the effective settings of an execution are reconstructed from, and the settings of
    # the user's profile, as evidence of what a query did not change.
    rows = client.fetch("SELECT name, value, default FROM system.settings WHERE alias_for = '' ORDER BY name")
    manifest = dict(schema_version=SCHEMA_VERSION, run_id=str(uuid.uuid4()), label=args.label, server=server,
                    reference_settings=reference,
                    # The functions that are not known to be deterministic, with their aliases.
                    volatile_functions=sorted(row['name'] for row in client.fetch(
                        'SELECT name FROM system.functions WHERE NOT is_aggregate AND coalesce(deterministic, 0) = 0 '
                        'UNION ALL SELECT name FROM system.functions WHERE alias_to IN (SELECT name FROM '
                        'system.functions WHERE NOT is_aggregate AND coalesce(deterministic, 0) = 0)')),
                    compiled_default_settings={row['name']: row['default'] for row in rows},
                    user_settings={row['name']: row['value'] for row in rows},
                    data_fingerprint=data_fingerprint(client, args.database),
                    source_commit=args.source_commit, database=args.database, queries=os.path.abspath(args.queries),
                    settings=settings, runs=args.runs, statements=[])
    texts = {}
    for name in sorted(f for f in os.listdir(args.queries) if f.endswith('.sql')):
        with open(os.path.join(args.queries, name)) as f:
            text = f.read()
        statement = dict(query=name[:-4], sha256=hashlib.sha256(text.encode()).hexdigest(), executions=[])
        problem = unsupported_statement(client.statements(text))
        if problem:
            statement['outcome'] = 'unsupported'
            statement['reason'] = problem
        else:
            statement['executions'] = [dict(run=run, query_id=str(uuid.uuid4()), outcome='pending', collection='pending')
                                       for run in range(1, args.runs + 1)]
            texts[statement['query']] = text
        manifest['statements'].append(statement)
    manifest_path = os.path.join(args.out, 'manifest.json')
    write_json(manifest_path, manifest)

    for statement in manifest['statements']:
        for execution in statement['executions']:
            comment = '%s %s run %d' % (args.label, statement['query'], execution['run'])
            code, _, err = client.execute(texts[statement['query']], settings_args + [
                '--query_id', execution['query_id'], '--log_comment', comment, '--format', 'Null'])
            execution['outcome'] = 'success' if code == 0 else 'execution_failure'
            if code != 0:
                execution['error_code'] = error_code(err)
                execution['collection'] = 'not applicable'
            write_json(manifest_path, manifest)
            print('%s run %d: %s' % (statement['query'], execution['run'], execution['outcome']), file=sys.stderr)

    flush_code, _, flush_err = client.execute('SYSTEM FLUSH LOGS')
    with open(os.path.join(args.out, 'observations.jsonl'), 'w') as out:
        for statement in manifest['statements']:
            for execution in statement['executions']:
                if execution['outcome'] != 'success':
                    continue
                if flush_code != 0:
                    execution['collection'] = 'failed: SYSTEM FLUSH LOGS: ' + flush_err.strip()[-300:]
                else:
                    execution['collection'] = collect(client, statement, execution, reference, manifest, out)
    write_json(manifest_path, manifest)


def q_error(estimated, actual):
    return max(estimated / actual, actual / estimated)


def geometric_mean(errors):
    return math.exp(sum(map(math.log, errors)) / len(errors))


def load(directory):
    with open(os.path.join(directory, 'manifest.json')) as f:
        manifest = json.load(f)
    runs = defaultdict(dict)
    with open(os.path.join(directory, 'observations.jsonl')) as f:
        for line in f:
            record = json.loads(line)
            runs[(record['query'], record['step'])][record['run']] = record
    return manifest, runs


def final_records(manifest, runs):
    """One record per step of each statement whose executions all succeeded and were collected: run 1's, with status
    `unstable` unless every execution has the step with the same kind, status, estimate and actual rows."""
    complete = {}
    for statement in manifest['statements']:
        executions = statement['executions']
        collected = lambda e: e['collection'] == 'ok' or e['collection'].startswith('ineligible')  # noqa: E731
        if executions and all(e['outcome'] == 'success' and collected(e) for e in executions):
            complete[statement['query']] = [e['run'] for e in executions]
    signature = lambda record: (record['kind'], record['status'], record['estimated'], record['actual'])  # noqa: E731
    records = []
    for (query, _), by_run in sorted(runs.items()):
        if query not in complete:
            continue
        reference = by_run.get(1) or next(iter(by_run.values()))
        record = dict(reference)
        if any(run not in by_run or signature(by_run[run]) != signature(reference) for run in complete[query]):
            record['status'] = 'unstable'
        records.append(record)
    return records


def classify(record):
    """`positive`, `zero_zero` or `one_zero` for an observed record, else None."""
    if record['status'] != 'observed':
        return None
    estimated, actual = record['estimated'], record['actual']
    if estimated == 0 and actual == 0:
        return 'zero_zero'
    if estimated == 0 or actual == 0:
        return 'one_zero'
    return 'positive'


def outcome_counts(manifest):
    outcomes = defaultdict(int)
    for statement in manifest['statements']:
        if statement.get('outcome') == 'unsupported':
            outcomes['unsupported'] += 1
        for execution in statement['executions']:
            collection = execution['collection']
            outcomes[execution['outcome'] + ('' if collection in ('ok', 'not applicable') else ', collection: ' + collection)] += 1
    return dict(sorted(outcomes.items()))


def summarize(directory):
    manifest, runs = load(directory)
    kinds = defaultdict(lambda: dict(statuses=defaultdict(int), errors=[], zero_zero=0, one_zero=0, high=0, low=0))
    for record in final_records(manifest, runs):
        kind = kinds[record['kind']]
        kind['statuses'][record['status']] += 1
        pair = classify(record)
        if pair in ('zero_zero', 'one_zero'):
            kind[pair] += 1
        elif pair == 'positive':
            error = q_error(record['estimated'], record['actual'])
            kind['errors'].append(error)
            if error > 10:
                kind['high' if record['estimated'] > record['actual'] else 'low'] += 1
    summary = {}
    for name, kind in sorted(kinds.items()):
        errors = kind['errors']
        summary[name] = dict(statuses=dict(sorted(kind['statuses'].items())), pairs=len(errors),
                             zero_zero=kind['zero_zero'], one_zero=kind['one_zero'],
                             geomean_q_error=geometric_mean(errors) if errors else None,
                             over_10x_high=kind['high'], over_10x_low=kind['low'])
    return dict(label=manifest['label'], run_id=manifest['run_id'], outcomes=outcome_counts(manifest), kinds=summary)


def command_report(args):
    print(json.dumps(summarize(args.directory), indent=1, sort_keys=True))


def by_query(manifest, runs):
    """Per (kind, query): the final records of that kind, by step id."""
    result = defaultdict(dict)
    for record in final_records(manifest, runs):
        result[(record['kind'], record['query'])][record['step']] = record
    return result


def errors_of(records):
    return [q_error(r['estimated'], r['actual']) for r in records if classify(r) == 'positive']


def compare_kind(kind, old, new, tolerance):
    """The verdict for one step kind and the line that explains it.

    Where the plan has the same steps of this kind in both runs (the same step ids), the steps are compared one by one:
    a step observed before and not now is lost evidence and fails the gate, as does a step that becomes a one-zero
    pair, instead of leaving the mean. Queries whose steps of this kind differ have different plans; their observed
    pairs are compared as a group of their own."""
    same, changed, lost, new_one_zero = [], [[], []], [], []
    for query in sorted({q for k, q in old if k == kind} | {q for k, q in new if k == kind}):
        before = old.get((kind, query), {})
        after = new.get((kind, query), {})
        if set(before) != set(after):
            # A changed plan keeps at least as many observed steps and one-zero pairs as few as before.
            count = lambda records, kinds: sum(1 for r in records if classify(r) in kinds)  # noqa: E731
            observed = ('positive', 'zero_zero', 'one_zero')
            if count(after.values(), observed) < count(before.values(), observed):
                lost.append(query)
            if count(after.values(), ('one_zero',)) > count(before.values(), ('one_zero',)):
                new_one_zero.append(query)
            changed[0] += errors_of(before.values())
            changed[1] += errors_of(after.values())
            continue
        for step, record in before.items():
            then, now = classify(record), classify(after[step])
            if then is not None and now is None:
                lost.append('%s/%s' % (query, step))
            elif now == 'one_zero' and then != 'one_zero':
                new_one_zero.append('%s/%s' % (query, step))
            elif then == 'positive' and now == 'positive':
                same.append((q_error(record['estimated'], record['actual']),
                             q_error(after[step]['estimated'], after[step]['actual'])))
    groups = [('same plan', [b for b, _ in same], [a for _, a in same]), ('changed plan', changed[0], changed[1])]
    verdict = 'fail' if lost or new_one_zero else 'insufficient evidence'
    parts = []
    for name, before, after in groups:
        if before and after:
            parts.append('%s %d -> %d pairs, q-error %.3f -> %.3f' % (
                name, len(before), len(after), geometric_mean(before), geometric_mean(after)))
        else:
            parts.append('%s %d -> %d pairs' % (name, len(before), len(after)))
        if verdict == 'fail' or min(len(before), len(after)) < MIN_PAIRS:
            continue
        verdict = 'fail' if geometric_mean(after) > geometric_mean(before) * (1 + tolerance) else 'pass'
    if lost:
        parts.append('lost ' + ','.join(lost))
    if new_one_zero:
        parts.append('new one-zero ' + ','.join(new_one_zero))
    return verdict, '%-24s %s: %s' % (kind, '; '.join(parts), verdict)


# Settings that two runs compare on purpose: the estimation models.
COMPARED_SETTINGS = {'query_plan_join_multi_key_model', 'query_plan_group_multi_key_model'}


def comparability(old, new):
    """Why two runs cannot be compared: different statements, data or settings other than the compared ones."""
    reasons = []
    if old['schema_version'] != new['schema_version']:
        reasons.append('different manifest schema versions')
    workload = lambda manifest: sorted((s['query'], s['sha256']) for s in manifest['statements'])  # noqa: E731
    if workload(old) != workload(new):
        reasons.append('different statements')
    if (old['database'], old['data_fingerprint']) != (new['database'], new['data_fingerprint']):
        reasons.append('different data')
    # The effective settings: the same defaults, and the same changes for every execution of a statement, apart from the
    # settings the runs compare on purpose.
    semantic = lambda settings: {n: v for n, v in settings.items() if n not in COMPARED_SETTINGS}  # noqa: E731
    if semantic(old['compiled_default_settings']) != semantic(new['compiled_default_settings']):
        reasons.append('different compiled default settings')

    def changes(manifest):
        return {(s['query'], e['run']): semantic(e['changed_settings'])
                for s in manifest['statements'] for e in s['executions'] if 'changed_settings' in e}
    old_changes = changes(old)
    new_changes = changes(new)
    # Every execution with settings evidence in both runs, repeat by repeat.
    if any(old_changes[execution] != new_changes[execution] for execution in old_changes.keys() & new_changes.keys()):
        reasons.append('different effective settings')
    return reasons


def command_compare(args):
    old_manifest, old_runs = load(args.old)
    new_manifest, new_runs = load(args.new)
    reasons = comparability(old_manifest, new_manifest)
    if reasons:
        print('not comparable: ' + '; '.join(reasons))
        sys.exit(2)

    # An execution that fails, or whose observations could not be collected, loses all of its evidence.
    succeeded = lambda s: bool(s['executions']) and all(  # noqa: E731
        e['outcome'] == 'success' and e['collection'] == 'ok' for e in s['executions'])
    new_statements = {s['query']: s for s in new_manifest['statements']}
    failures = ['%s fails or is not collected now' % s['query'] for s in old_manifest['statements']
                if succeeded(s) and not succeeded(new_statements[s['query']])]

    old = by_query(old_manifest, old_runs)
    new = by_query(new_manifest, new_runs)
    verdicts = []
    for kind in sorted({k for k, _ in old} | {k for k, _ in new}):
        verdict, line = compare_kind(kind, old, new, args.tolerance)
        verdicts.append(verdict)
        print(line)
    for failure in failures:
        print(failure)
    if failures or 'fail' in verdicts:
        print('gate: fail')
        sys.exit(1)
    # A kind with too few pairs does not decide; the gate needs at least one kind with enough.
    if 'pass' not in verdicts:
        print('gate: insufficient evidence')
        sys.exit(2)
    print('gate: pass')


def main():
    args = parse_args()
    {'run': command_run, 'report': command_report, 'compare': command_compare}[args.command](args)


if __name__ == '__main__':
    main()
