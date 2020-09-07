# -*- coding: utf-8 -*-

'''
    Rules for commit messages, branch names and everything:

    - All important(!) commits to master branch must originate from pull-requests.
    - All pull-requests must be squash-merged or explicitly merged without rebase.
    - All pull-requests to master must have at least one label prefixed with `pr-`.
    - Labels that require pull-request to be backported must be red colored (#ff0000).
    - Release branch name must be of form `YY.NUMBER`.
    - All release branches must be forked directly from the master branch and never be merged back,
      or merged with any other branches based on the master branch (including master branch itself).

    Output of this script:

    - Commits without references from pull-requests.
    - Pull-requests to master without proper labels.
    - Pull-requests that need to be backported, with statuses per release branch.

'''

from . import local, query
from . import parser as parse_description

import argparse
import re
import sys

try:
    from termcolor import colored  # `pip install termcolor`
except ImportError:
    sys.exit("Package 'termcolor' not found. Try run: `pip3 install [--user] termcolor`")


CHECK_MARK = colored('🗸', 'green')
CROSS_MARK = colored('🗙', 'red')
BACKPORT_LABEL_MARK = colored('🏷', 'yellow')
CONFLICT_LABEL_MARK = colored('☁', 'yellow')
NO_BACKPORT_LABEL_MARK = colored('━', 'yellow')
CLOCK_MARK = colored('↻', 'cyan')


parser = argparse.ArgumentParser(description='Helper for the ClickHouse Release machinery')
parser.add_argument('--repo', '-r', type=str, default='', metavar='PATH',
    help='path to the root of the ClickHouse repository')
parser.add_argument('--remote', type=str, default='origin',
    help='remote name of the "ClickHouse/ClickHouse" upstream')
parser.add_argument('--token', type=str, required=True,
    help='token for Github access')
parser.add_argument('--login', type=str,
    help='filter authorship by login')
parser.add_argument('--auto-label', action='store_true', dest='autolabel', default=True,
    help='try to automatically parse PR description and put labels')

# Either select last N release branches, or specify them manually.
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-n', type=int, default=3, dest='number',
    help='number of last release branches to consider')
group.add_argument('--branch', type=str, action='append', metavar='BRANCH',
    help='specific release branch name to consider')

args = parser.parse_args()

github = query.Query(args.token, 30)
repo = local.Local(args.repo, args.remote, github.get_default_branch())

if not args.branch:
    release_branches = repo.get_release_branches()[-args.number:] # [(branch name, base)]
else:
    release_branches = []
    all_release_branches = repo.get_release_branches()
    for branch in all_release_branches:
        if branch[0] in args.branch:
            release_branches.append(branch)

if not release_branches:
    sys.exit('No release branches found!')
else:
    print('Found release branches:')
    for branch in release_branches:
        print(f'{CHECK_MARK} {branch[0]} forked from {branch[1]}')

first_commit = release_branches[0][1]
pull_requests = github.get_pull_requests(first_commit, args.login)
good_commits = set(pull_request['mergeCommit']['oid'] for pull_request in pull_requests)

bad_commits = [] # collect and print them in the end
from_commit = repo.get_head_commit()
for i in reversed(range(len(release_branches))):
    for commit in repo.iterate(from_commit, release_branches[i][1]):
        if str(commit) not in good_commits and commit.author.name != 'robot-clickhouse':
            bad_commits.append(commit)

    from_commit = release_branches[i][1]

members = set(github.get_members("ClickHouse", "ClickHouse"))
def print_responsible(pull_request):
    if "author" not in pull_request or pull_request["author"] is None:
        return "No author"
    if pull_request["author"]["login"] in members:
        return colored(pull_request["author"]["login"], 'green')
    elif pull_request["mergedBy"]["login"] in members:
        return f'{pull_request["author"]["login"]} → {colored(pull_request["mergedBy"]["login"], "green")}'
    else:
        return f'{pull_request["author"]["login"]} → {pull_request["mergedBy"]["login"]}'

LABEL_NO_BACKPORT = 'pr-no-backport'
bad_pull_requests = []  # collect and print if not empty
need_backporting = []
for pull_request in pull_requests:

    def find_label():
        labels = github.get_labels(pull_request)
        backport_allowed = LABEL_NO_BACKPORT not in map(lambda label: label['name'], labels)
        for label in labels:
            if label['name'].startswith('pr-'):
                if label['color'] == 'ff0000' and backport_allowed:
                    need_backporting.append(pull_request)
                return True
        return False

    label_found = find_label()

    if not label_found and args.autolabel:
        print(f"Trying to auto-label pull-request: {pull_request['number']}")
        description = parse_description.Description(pull_request)
        if description.label_name:
            github.set_label(pull_request, description.label_name)
            label_found = find_label()

    if not label_found:
        bad_pull_requests.append(pull_request)

if bad_pull_requests:
    print('\nPull-requests without description label:')
    for bad in reversed(sorted(bad_pull_requests, key = lambda x : x['number'])):
        print(f'{CROSS_MARK} {bad["number"]}: {bad["url"]} ({print_responsible(bad)})')

# FIXME: compatibility logic, until the direct modification of master is not prohibited.
if bad_commits and not args.login:
    print('\nCommits not referenced by any pull-request:')

    for bad in bad_commits:
        print(f'{CROSS_MARK} {bad} {bad.author}')

# TODO: check backports.
if need_backporting:
    re_vlabel = re.compile(r'^v\d+\.\d+$')
    re_vlabel_backported = re.compile(r'^v\d+\.\d+-backported$')
    re_vlabel_conflicts = re.compile(r'^v\d+\.\d+-conflicts$')
    re_vlabel_no_backport = re.compile(r'^v\d+\.\d+-no-backport$')

    print('\nPull-requests need to be backported:')
    for pull_request in reversed(sorted(need_backporting, key=lambda x: x['number'])):
        targets = []  # use common list for consistent order in output
        good = set()
        backport_labeled = set()
        conflict_labeled = set()
        no_backport_labeled = set()
        wait = set()

        for branch in release_branches:
            if repo.comparator(branch[1]) < repo.comparator(pull_request['mergeCommit']['oid']):
                targets.append(branch[0])

                # FIXME: compatibility logic - check for a manually set label, that indicates status 'backported'.
                # FIXME: O(n²) - no need to iterate all labels for every `branch`
                for label in github.get_labels(pull_request):
                    if re_vlabel.match(label['name']) or re_vlabel_backported.match(label['name']):
                        if f'v{branch[0]}' == label['name'] or f'v{branch[0]}-backported' == label['name']:
                            backport_labeled.add(branch[0])
                    if re_vlabel_conflicts.match(label['name']):
                        if f'v{branch[0]}-conflicts' == label['name']:
                            conflict_labeled.add(branch[0])
                    if re_vlabel_no_backport.match(label['name']):
                        if f'v{branch[0]}-no-backport' == label['name']:
                            no_backport_labeled.add(branch[0])

        for event in github.get_timeline(pull_request):
            if(event['isCrossRepository'] or
               event['target']['number'] != pull_request['number'] or
               event['source']['baseRefName'] not in targets):
                continue

            found_label = False
            for label in github.get_labels(event['source']):
                if label['name'] == 'pr-backport':
                    found_label = True
                    break
            if not found_label:
                continue

            if event['source']['merged']:
                good.add(event['source']['baseRefName'])
            else:
                wait.add(event['source']['baseRefName'])

        # print pull-request's status
        if len(good) + len(backport_labeled) + len(conflict_labeled) + len(no_backport_labeled) == len(targets):
            print(f'{CHECK_MARK}', end=' ')
        else:
            print(f'{CROSS_MARK}', end=' ')
        print(f'{pull_request["number"]}', end=':')
        for target in targets:
            if target in good:
                print(f'\t{CHECK_MARK} {target}', end='')
            elif target in backport_labeled:
                print(f'\t{BACKPORT_LABEL_MARK} {target}', end='')
            elif target in conflict_labeled:
                print(f'\t{CONFLICT_LABEL_MARK} {target}', end='')
            elif target in no_backport_labeled:
                print(f'\t{NO_BACKPORT_LABEL_MARK} {target}', end='')
            elif target in wait:
                print(f'\t{CLOCK_MARK} {target}', end='')
            else:
                print(f'\t{CROSS_MARK} {target}', end='')
        print(f'\t{pull_request["url"]} ({print_responsible(pull_request)})')

# print legend
print('\nLegend:')
print(f'{CHECK_MARK} - good')
print(f'{CROSS_MARK} - bad')
print(f'{BACKPORT_LABEL_MARK} - backport is detected via label')
print(f'{CONFLICT_LABEL_MARK} - backport conflict is detected via label')
print(f'{NO_BACKPORT_LABEL_MARK} - backport to this release is not needed')
print(f'{CLOCK_MARK} - backport is waiting to merge')

# print API costs
print('\nGitHub API total costs per query:')
for name, value in github.api_costs.items():
    print(f'{name} : {value}')
