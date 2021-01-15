# -*- coding: utf-8 -*-

'''
Backports changes from PR to release branch.
Requires multiple separate runs as part of the implementation.

First run should do the following:
1. Merge release branch with a first parent of merge-commit of PR (using 'ours' strategy). (branch: backport/{branch}/{pr})
2. Create temporary branch over merge-commit to use it for PR creation. (branch: cherrypick/{merge_commit})
3. Create PR from temporary branch to backport branch (emulating cherry-pick).

Second run checks PR from previous run to be merged or at least being mergeable. If it's not merged then try to merge it.

Third run creates PR from backport branch (with merged previous PR) to release branch.
'''

from clickhouse.utils.github.query import Query as RemoteRepo

import argparse
from enum import Enum
import logging
import os
import subprocess
import sys


class CherryPick:
    class Status(Enum):
        DISCARDED = 'discarded'
        NOT_INITIATED = 'not started'
        FIRST_MERGEABLE = 'waiting for 1st stage'
        FIRST_CONFLICTS = 'conflicts on 1st stage'
        SECOND_MERGEABLE = 'waiting for 2nd stage'
        SECOND_CONFLICTS = 'conflicts on 2nd stage'
        MERGED = 'backported'

    def _run(self, args):
        out = subprocess.check_output(args).rstrip()
        logging.debug(out)
        return out

    def __init__(self, token, owner, name, team, pr_number, target_branch):
        self._gh = RemoteRepo(token, owner=owner, name=name, team=team)
        self._pr = self._gh.get_pull_request(pr_number)

        self.ssh_url = self._gh.ssh_url

        # TODO: check if pull-request is merged.

        self.merge_commit_oid = self._pr['mergeCommit']['oid']

        self.target_branch = target_branch
        self.backport_branch = 'backport/{branch}/{pr}'.format(branch=target_branch, pr=pr_number)
        self.cherrypick_branch = 'cherrypick/{branch}/{oid}'.format(branch=target_branch, oid=self.merge_commit_oid)

    def getCherryPickPullRequest(self):
        return self._gh.find_pull_request(base=self.backport_branch, head=self.cherrypick_branch)

    def createCherryPickPullRequest(self, repo_path):
        DESCRIPTION = (
            'This pull-request is a first step of an automated backporting.\n'
            'It contains changes like after calling a local command `git cherry-pick`.\n'
            'If you intend to continue backporting this changes, then resolve all conflicts if any.\n'
            'Otherwise, if you do not want to backport them, then just close this pull-request.\n'
            '\n'
            'The check results does not matter at this step - you can safely ignore them.\n'
            'Also this pull-request will be merged automatically as it reaches the mergeable state, but you always can merge it manually.\n'
        )

        # FIXME: replace with something better than os.system()
        git_prefix = ['git', '-C', repo_path, '-c', 'user.email=robot-clickhouse@yandex-team.ru', '-c', 'user.name=robot-clickhouse']
        base_commit_oid = self._pr['mergeCommit']['parents']['nodes'][0]['oid']

        # Create separate branch for backporting, and make it look like real cherry-pick.
        self._run(git_prefix + ['checkout', '-f', self.target_branch])
        self._run(git_prefix + ['checkout', '-B', self.backport_branch])
        self._run(git_prefix + ['merge', '-s', 'ours', '--no-edit', base_commit_oid])

        # Create secondary branch to allow pull request with cherry-picked commit.
        self._run(git_prefix + ['branch', '-f', self.cherrypick_branch, self.merge_commit_oid])

        self._run(git_prefix + ['push', '-f', 'origin', '{branch}:{branch}'.format(branch=self.backport_branch)])
        self._run(git_prefix + ['push', '-f', 'origin', '{branch}:{branch}'.format(branch=self.cherrypick_branch)])

        # Create pull-request like a local cherry-pick
        pr = self._gh.create_pull_request(source=self.cherrypick_branch, target=self.backport_branch,
                                          title='Cherry pick #{number} to {target}: {title}'.format(
                                              number=self._pr['number'], target=self.target_branch,
                                              title=self._pr['title'].replace('"', '\\"')),
                                          description='Original pull-request #{}\n\n{}'.format(self._pr['number'], DESCRIPTION))

        # FIXME: use `team` to leave a single eligible assignee.
        self._gh.add_assignee(pr, self._pr['author'])
        self._gh.add_assignee(pr, self._pr['mergedBy'])

        self._gh.set_label(pr, "do not test")
        self._gh.set_label(pr, "pr-cherrypick")

        return pr

    def mergeCherryPickPullRequest(self, cherrypick_pr):
        return self._gh.merge_pull_request(cherrypick_pr['id'])

    def getBackportPullRequest(self):
        return self._gh.find_pull_request(base=self.target_branch, head=self.backport_branch)

    def createBackportPullRequest(self, cherrypick_pr, repo_path):
        DESCRIPTION = (
            'This pull-request is a last step of an automated backporting.\n'
            'Treat it as a standard pull-request: look at the checks and resolve conflicts.\n'
            'Merge it only if you intend to backport changes to the target branch, otherwise just close it.\n'
        )

        git_prefix = ['git', '-C', repo_path, '-c', 'user.email=robot-clickhouse@yandex-team.ru', '-c', 'user.name=robot-clickhouse']

        pr_title = 'Backport #{number} to {target}: {title}'.format(
            number=self._pr['number'], target=self.target_branch,
            title=self._pr['title'].replace('"', '\\"'))

        self._run(git_prefix + ['checkout', '-f', self.backport_branch])
        self._run(git_prefix + ['pull', '--ff-only', 'origin', self.backport_branch])
        self._run(git_prefix + ['reset', '--soft', self._run(git_prefix + ['merge-base', 'origin/' + self.target_branch, self.backport_branch])])
        self._run(git_prefix + ['commit', '-a', '--allow-empty', '-m', pr_title])
        self._run(git_prefix + ['push', '-f', 'origin', '{branch}:{branch}'.format(branch=self.backport_branch)])

        pr = self._gh.create_pull_request(source=self.backport_branch, target=self.target_branch, title=pr_title,
                                          description='Original pull-request #{}\nCherry-pick pull-request #{}\n\n{}'.format(self._pr['number'], cherrypick_pr['number'], DESCRIPTION))

        # FIXME: use `team` to leave a single eligible assignee.
        self._gh.add_assignee(pr, self._pr['author'])
        self._gh.add_assignee(pr, self._pr['mergedBy'])

        self._gh.set_label(pr, "pr-backport")

        return pr

    def execute(self, repo_path, dry_run=False):
        pr1 = self.getCherryPickPullRequest()
        if not pr1:
            if not dry_run:
                pr1 = self.createCherryPickPullRequest(repo_path)
                logging.debug('Created PR with cherry-pick of %s to %s: %s', self._pr['number'], self.target_branch, pr1['url'])
            else:
                return CherryPick.Status.NOT_INITIATED
        else:
            logging.debug('Found PR with cherry-pick of %s to %s: %s', self._pr['number'], self.target_branch, pr1['url'])

        if not pr1['merged'] and pr1['mergeable'] == 'MERGEABLE' and not pr1['closed']:
            if not dry_run:
                pr1 = self.mergeCherryPickPullRequest(pr1)
                logging.debug('Merged PR with cherry-pick of %s to %s: %s', self._pr['number'], self.target_branch, pr1['url'])

        if not pr1['merged']:
            logging.debug('Waiting for PR with cherry-pick of %s to %s: %s', self._pr['number'], self.target_branch, pr1['url'])

            if pr1['closed']:
                return CherryPick.Status.DISCARDED
            elif pr1['mergeable'] == 'CONFLICTING':
                return CherryPick.Status.FIRST_CONFLICTS
            else:
                return CherryPick.Status.FIRST_MERGEABLE

        pr2 = self.getBackportPullRequest()
        if not pr2:
            if not dry_run:
                pr2 = self.createBackportPullRequest(pr1, repo_path)
                logging.debug('Created PR with backport of %s to %s: %s', self._pr['number'], self.target_branch, pr2['url'])
            else:
                return CherryPick.Status.FIRST_MERGEABLE
        else:
            logging.debug('Found PR with backport of %s to %s: %s', self._pr['number'], self.target_branch, pr2['url'])

        if pr2['merged']:
            return CherryPick.Status.MERGED
        elif pr2['closed']:
            return CherryPick.Status.DISCARDED
        elif pr2['mergeable'] == 'CONFLICTING':
            return CherryPick.Status.SECOND_CONFLICTS
        else:
            return CherryPick.Status.SECOND_MERGEABLE


if __name__ == "__main__":
    logging.basicConfig(format='%(message)s', stream=sys.stdout, level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('--token',  '-t', type=str, required=True, help='token for Github access')
    parser.add_argument('--pr',           type=str, required=True, help='PR# to cherry-pick')
    parser.add_argument('--branch', '-b', type=str, required=True, help='target branch name for cherry-pick')
    parser.add_argument('--repo',   '-r', type=str, required=True, help='path to full repository', metavar='PATH')
    args = parser.parse_args()

    cp = CherryPick(args.token, 'ClickHouse', 'ClickHouse', 'core', args.pr, args.branch)
    cp.execute(args.repo)
