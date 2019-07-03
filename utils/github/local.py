# -*- coding: utf-8 -*-

# `pip install …`
import git # gitpython

import functools
import os
import re


class Local:
    '''Implements some useful methods atop of the local repository
    '''
    RE_STABLE_REF = re.compile(r'^refs/remotes/.+/\d+\.\d+$')

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
    def get_stables(self):
        stables = []

        for stable in [r for r in self._remote.refs if Local.RE_STABLE_REF.match(r.path)]:
            base = self._repo.merge_base(self._default, self._repo.commit(stable))
            if not base:
                print(f'Branch {stable.path} is not based on branch {self._default}. Ignoring.')
            elif len(base) > 1:
                print(f'Branch {stable.path} has more than one base commit. Ignoring.')
            else:
                stables.append((os.path.basename(stable.name), base[0]))

        return sorted(stables, key=lambda x : self.comparator(x[1]))
