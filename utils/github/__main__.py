# -*- coding: utf-8 -*-

'''
    Rules for commit messages, branch names and everything:

    - All(!) commits to master branch must originate from pull-requests.
    - All pull-requests must be squash-merged or explicitly merged without rebase.
    - All pull-requests to master must have at least one label prefixed with `pr-`.
    - Labels that require pull-request to be backported must be red colored (#ff0000).
    - Stable branch name must be of form `YY.NUMBER`.
    - All stable branches must be forked directly from the master branch and never be merged back,
      or merged with any other branches based on the master branch (including master branch itself).

    Output of this script:

    - Commits without references from pull-requests.
    - Pull-requests to master without proper labels.
    - Pull-requests that need to be backported.

'''

from . import local, query

import argparse
import re
import sys

CHECK_MARK = '🗸'
CROSS_MARK = '🗙'
LABEL_MARK = '🏷'


parser = argparse.ArgumentParser(description='Helper for the ClickHouse Release machinery')
parser.add_argument('--repo', '-r', type=str, default='', metavar='PATH',
    help='path to the root of the ClickHouse repository')
parser.add_argument('--remote', type=str, default='origin',
    help='remote name of the "yandex/ClickHouse" upstream')
parser.add_argument('-n', type=int, default=3, dest='number',
    help='number of last stable branches to consider')
parser.add_argument('--token', type=str, required=True,
    help='token for Github access')
parser.add_argument('--login', type = str,
    help='filter authorship by login')

args = parser.parse_args()

github = query.Query(args.token)
repo = local.Local(args.repo, args.remote, github.get_default_branch())

stables = repo.get_stables()[-args.number:] # [(branch, base)]
if not stables:
    sys.exit('No stable branches found!')
else:
    print('Found stable branches:')
    for stable in stables:
        print(f'{CHECK_MARK} {stable[0]} forked from {stable[1]}')

first_commit = stables[0][1]
pull_requests = github.get_pull_requests(first_commit, args.login)
good_commits = set(pull_request['mergeCommit']['oid'] for pull_request in pull_requests)

bad_commits = [] # collect and print them in the end
from_commit = repo.get_head_commit()
for i in reversed(range(len(stables))):
    for commit in repo.iterate(from_commit, stables[i][1]):
        if str(commit) not in good_commits and commit.author.name != 'robot-clickhouse':
            bad_commits.append(commit)

    from_commit = stables[i][1]

bad_pull_requests = []  # collect and print if not empty
need_backporting = []
for pull_request in pull_requests:
    label_found = False

    for label in github.get_labels(pull_request):
        if label['name'].startswith('pr-'):
            label_found = True
            if label['color'] == 'ff0000':
                need_backporting.append(pull_request)
            break

    if not label_found:
        bad_pull_requests.append(pull_request)

if bad_pull_requests:
    print('\nPull-requests without description label:')
    for bad in reversed(sorted(bad_pull_requests, key = lambda x : x['number'])):
        print(f'{CROSS_MARK} {bad["number"]}: {bad["url"]}')

# FIXME: compatibility logic, until the direct modification of master is not prohibited.
if bad_commits:
    print('\nCommits not referenced by any pull-request:')

    for bad in bad_commits:
        print(f'{CROSS_MARK} {bad} {bad.author}')

# TODO: check backports.
if need_backporting:
    re_vlabel = re.compile(r'^v\d+\.\d+$')
    re_stable_num = re.compile(r'\d+\.\d+$')

    print('\nPull-requests need to be backported:')
    for pull_request in reversed(sorted(need_backporting, key=lambda x: x['number'])):
        targets = []  # use common list for consistent order in output
        good = set()

        for stable in stables:
            if repo.comparator(stable[1]) < repo.comparator(pull_request['mergeCommit']['oid']):
                targets.append(stable)

                # FIXME: compatibility logic - check for a manually set label, that indicates status 'backported'.
                # FIXME: O(n²) - no need to iterate all labels for every `stable`
                for label in github.get_labels(pull_request):
                    if re_vlabel.match(label['name']):
                        stable_num = re_stable_num.search(stable[0].name)
                        if f'v{stable_num[0]}' == label['name']:
                            good.add(stable)

        # print pull-request's status
        if len(good) == len(targets):
            print(f'{CHECK_MARK}', end=' ')
        else:
            print(f'{CROSS_MARK}', end=' ')
        print(f'{pull_request["number"]}', end=':')
        for target in targets:
            if target in good:
                print(f'\t{LABEL_MARK} {target[0]}', end='')
            else:
                print(f'\t{CROSS_MARK} {target[0]}', end='')
        print(f'\t({pull_request["mergeCommit"]["author"]["name"]}) {pull_request["url"]}')

# print legend
print('\nLegend:')
print(f'{CHECK_MARK} - good')
print(f'{CROSS_MARK} - bad')
print(f'{LABEL_MARK} - backport is detected via label')
