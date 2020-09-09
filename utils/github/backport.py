# -*- coding: utf-8 -*-

from query import Query as RemoteRepo
from local import BareRepository as LocalRepo
import cherrypick

import argparse
import logging
import re
import sys


class Backport:
    def __init__(self, token, owner, name, team):
        '''
        `refs` is a list of (ref_path, base_commit) sorted by ancestry starting from the least recent ref.
        '''
        self._gh = RemoteRepo(token, owner=owner, name=name, team=team, max_page_size=30)
        self.default_branch_name = self._gh.default_branch

    def getPullRequests(self, from_commit):
        return self._gh.get_pull_requests(from_commit)


def run(token, repo_bare, til, number, run_cherrypick):
    bp = Backport(token, 'ClickHouse', 'ClickHouse', 'core')
    repo = LocalRepo(repo_bare, bp.default_branch_name)

    branches = repo.get_release_branches()[-number:]  # [(branch_name, base_commit)]

    if not branches:
        logging.info('No release branches found!')
        return

    for branch in branches:
        logging.info('Found release branch: %s', branch[0])

    if not til:
        til = branches[0][1]
    prs = bp.getPullRequests(til)

    backport_map = {}

    RE_MUST_BACKPORT = re.compile(r'^v(\d+\.\d+)-must-backport$')
    RE_NO_BACKPORT = re.compile(r'^v(\d+\.\d+)-no-backport$')

    # pull-requests are sorted by ancestry from the least recent.
    for pr in prs:
        while repo.comparator(branches[-1][1]) >= repo.comparator(pr['mergeCommit']['oid']):
            branches.pop()

        assert len(branches)

        branch_set = set([branch[0] for branch in branches])

        # First pass. Find all must-backports
        for label in pr['labels']['nodes']:
            if label['name'].startswith('pr-') and label['color'] == 'ff0000':
                backport_map[pr['number']] = branch_set.copy()
                continue
            m = RE_MUST_BACKPORT.match(label['name'])
            if m:
                if pr['number'] not in backport_map:
                    backport_map[pr['number']] = set()
                backport_map[pr['number']].add(m.group(1))

        # Second pass. Find all no-backports
        for label in pr['labels']['nodes']:
            if label['name'] == 'pr-no-backport' and pr['number'] in backport_map:
                del backport_map[pr['number']]
                break
            m = RE_NO_BACKPORT.match(label['name'])
            if m and pr['number'] in backport_map and m.group(1) in backport_map[pr['number']]:
                backport_map[pr['number']].remove(m.group(1))

    for pr, branches in backport_map.items():
        logging.info('PR #%s needs to be backported to:', pr)
        for branch in branches:
            logging.info('\t%s %s', branch, run_cherrypick(token, pr, branch))

    # print API costs
    logging.info('\nGitHub API total costs per query:')
    for name, value in bp._gh.api_costs.items():
        logging.info('%s : %s', name, value)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--token',     type=str, required=True, help='token for Github access')
    parser.add_argument('--repo-bare', type=str, required=True, help='path to bare repository', metavar='PATH')
    parser.add_argument('--repo-full', type=str, required=True, help='path to full repository', metavar='PATH')
    parser.add_argument('--til',       type=str,                help='check PRs from HEAD til this commit', metavar='COMMIT')
    parser.add_argument('-n',          type=int, dest='number', help='number of last release branches to consider')
    parser.add_argument('--dry-run',   action='store_true',     help='do not create or merge any PRs', default=False)
    parser.add_argument('--verbose', '-v', action='store_true', help='more verbose output', default=False)
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(format='%(message)s', stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(message)s', stream=sys.stdout, level=logging.INFO)

    cherrypick_run = lambda token, pr, branch: cherrypick.run(token, pr, branch, args.repo_full, args.dry_run)
    run(args.token, args.repo_bare, args.til, args.number, cherrypick_run)
