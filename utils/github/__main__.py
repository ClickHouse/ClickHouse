#!/usr/bin/env python3
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

'''

from . import local, query

import argparse
import sys


parser = argparse.ArgumentParser(description='Helper for the ClickHouse Release machinery')
parser.add_argument('--repo', '-r', type=str, default='', metavar='PATH',
    help='path to the root of the ClickHouse repository')
parser.add_argument('--remote', type=str, default='origin',
    help='remote name of the "yandex/ClickHouse" upstream')
parser.add_argument('-n', type=int, default=3, dest='number',
    help='number of last stable branches to consider')
parser.add_argument('--token', type=str, required=True,
    help='token for Github access')

args = parser.parse_args()

github = query.Query(args.token)
repo = local.Local(args.repo, args.remote, github.get_default_branch())

stables = repo.get_stables()[-args.number:]
if not stables:
    sys.exit('No stable branches found!')
else:
    print('Found stable branches:')
    for stable in stables:
        print(f'{stable[0]} forked from {stable[1]}')

first_commit = max(repo.get_first_commit(), stables[0][1], key=repo.comparator)
pull_requests = github.get_pull_requests(first_commit)
good_commits = set(oid[0] for oid in pull_requests.values())

print()
print('Problems:')

# Iterate all local commits on portions: from HEAD to 1st recent stable base, then to 2nd recent base, and so on.
# It will help to detect necessity to cherry-pick or backport something to previous stable branches.
from_commit = repo.get_head_commit()
for i in reversed(range(len(stables))):
    if repo.comparator(stables[i][1]) < repo.comparator(first_commit):
        break

    for commit in repo.iterate(from_commit, stables[i][1]):
        if str(commit) not in good_commits:
            print(f'commit {commit} is not referenced by any pull-request', file=sys.stderr)

    from_commit = stables[i][1]

for num, value in pull_requests.items():
    label_found = False

    for label in value[1]:
        if label.startswith('pr-'):
            label_found = True
            if label in ['pr-bugfix', 'pr-performance']:
                print(f'pull-request {num} should be backported')
            break

    if not label_found:
        print(f'pull-request {num} has no description label', file=sys.stderr)
