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

from query import Query as RemoteRepo

import argparse
import os
import sys
import time


class CherryPick:
    def __init__(self, token, owner, name, team, pr_number, target_branch):
        self._gh = RemoteRepo(token, owner=owner, name=name, team=team)
        self._pr = self._gh.get_pull_request(pr_number)

        # TODO: check if pull-request is merged.

        self.merge_commit_oid = self._pr['mergeCommit']['oid']

        self.target_branch = target_branch
        self.backport_branch = 'backport/{branch}/{pr}'.format(branch=target_branch, pr=pr_number)
        self.cherrypick_branch = 'cherrypick/{branch}/{oid}'.format(branch=target_branch, oid=self.merge_commit_oid)

    def getCherryPickPullRequest(self):
        return self._gh.find_pull_request(base=self.backport_branch, head=self.cherrypick_branch)

    def createCherryPickPullRequest(self, repo_path):
        # FIXME: replace with something better than os.system()
        git_prefix = 'git -C {} -c "user.email=robot-clickhouse@yandex-team.ru" -c "user.name=robot-clickhouse" '.format(repo_path)
        base_commit_oid = self._pr['mergeCommit']['parents']['nodes'][0]['oid']

        # Create separate branch for backporting, and make it look like real cherry-pick.
        os.system(git_prefix + 'checkout -f ' + self.target_branch)
        os.system(git_prefix + 'checkout -B ' + self.backport_branch)
        os.system(git_prefix + 'merge -s ours --no-edit ' + base_commit_oid)

        # Create secondary branch to allow pull request with cherry-picked commit.
        os.system(git_prefix + 'branch -f {} {}'.format(self.cherrypick_branch, self.merge_commit_oid))

        os.system(git_prefix + 'push -f origin {branch}:{branch}'.format(branch=self.backport_branch))
        os.system(git_prefix + 'push -f origin {branch}:{branch}'.format(branch=self.cherrypick_branch))

        # Create pull-request like a local cherry-pick
        pr = self._gh.create_pull_request(source=self.cherrypick_branch, target=self.backport_branch,
            title='Cherry pick #{number} to {target}: {title}'.format(
                number=self._pr['number'], target=self.target_branch, title=self._pr['title'].replace('"', '\\"')),
            description='Original pull-request #{}'.format(self._pr['number']))

        self._gh.add_assignee(pr, self._pr['author'])
        self._gh.add_assignee(pr, self._pr['mergedBy'])
        self._gh.set_label(pr, "do not test")
        self._gh.set_label(pr, "pr-backport")

        return pr

    def mergeCherryPickPullRequest(self, cherrypick_pr):
        return self._gh.merge_pull_request(cherrypick_pr['id'])

    def getBackportPullRequest(self):
        return self._gh.find_pull_request(base=self.target_branch, head=self.backport_branch)

    def createBackportPullRequest(self, cherrypick_pr):
        pr = self._gh.create_pull_request(source=self.backport_branch, target=self.target_branch,
            title='Backport #{number} to {target}: {title}'.format(
                number=self._pr['number'], target=self.target_branch, title=self._pr['title'].replace('"', '\\"')),
            description='Original pull-request #{}\nCherry-pick pull-request #{}'.format(self._pr['number'], cherrypick_pr['number']))

        self._gh.add_assignee(pr, self._pr['author'])
        self._gh.add_assignee(pr, self._pr['mergedBy'])
        self._gh.set_label(pr, "pr-backport")

        return pr


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', type=str, required=True, help='token for Github access')
    parser.add_argument('--number', type=str, required=True, help='number of PR to cherrypick')
    parser.add_argument('--target', type=str, required=True, help='branch name to cherrypick to')
    parser.add_argument('--repo', type=str, required=True, help='path to full repository')
    args = parser.parse_args()

    cp = CherryPick(args.token, 'ClickHouse', 'ClickHouse', 'core', args.number, args.target)

    pr1 = cp.getCherryPickPullRequest()
    if not pr1:
        pr1 = cp.createCherryPickPullRequest(args.repo)

    if not pr1['merged'] and pr1['mergeable'] == 'MERGEABLE'and not pr1['closed']:
        pr1 = cp.mergeCherryPickPullRequest(pr1)

    if not pr1['merged']:
        print (pr1)
        sys.exit()  # cherry-pick is rejected

    pr2 = cp.getBackportPullRequest()
    if not pr2:
        pr2 = cp.createBackportPullRequest(pr1)

    print (pr2)
