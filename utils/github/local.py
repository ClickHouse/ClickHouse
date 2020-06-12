# -*- coding: utf-8 -*-

try:
    import git # `pip3 install gitpython`
except ImportError:
    sys.exit("Package 'gitpython' not found. Try run: `pip3 install [--user] gitpython`")

import functools
import os
import re


class Local:
    '''Implements some useful methods atop of the local repository
    '''
    RE_RELEASE_BRANCH_REF = re.compile(r'^refs/remotes/.+/\d+\.\d+$')

    def __init__(self, repo_path, remote_name, default_branch_name):
        self._repo = git.Repo(repo_path, search_parent_directories=(not repo_path))
        self._remote = self._repo.remotes[remote_name]
        self._default = self._remote.refs[default_branch_name]

        # public key comparator
        def cmp(x, y):
            if x == y:
                return 0
            if self._repo.is_ancestor(x, y):
                return -1
            else:
                return 1
        self.comparator = functools.cmp_to_key(cmp)

    def get_head_commit(self):
        return self._repo.commit(self._default)

    def iterate(self, begin, end):
        rev_range = '{}...{}'.format(begin, end)
        for commit in self._repo.iter_commits(rev_range, first_parent=True):
            yield commit

    ''' Returns sorted list of tuples:
         * remote branch (git.refs.remote.RemoteReference),
         * base commit (git.Commit),
         * head (git.Commit)).
        List is sorted by commits in ascending order.
    '''
    def get_release_branches(self):
        release_branches = []

        for branch in [r for r in self._remote.refs if Local.RE_RELEASE_BRANCH_REF.match(r.path)]:
            base = self._repo.merge_base(self._default, self._repo.commit(branch))
            if not base:
                print(f'Branch {branch.path} is not based on branch {self._default}. Ignoring.')
            elif len(base) > 1:
                print(f'Branch {branch.path} has more than one base commit. Ignoring.')
            else:
                release_branches.append((os.path.basename(branch.name), base[0]))

        return sorted(release_branches, key=lambda x : self.comparator(x[1]))
