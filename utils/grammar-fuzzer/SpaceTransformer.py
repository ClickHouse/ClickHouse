# -*- coding: utf-8 -*-

from grammarinator.runtime.tree import *

from itertools import tee, islice, zip_longest
import random


def single_line_whitespace(node):
    return _whitespace(node, ' \t')


def multi_line_whitespace(node):
    return _whitespace(node, ' \t\r\n')


def _whitespace(node, symbols):
    for child in node.children:
        _whitespace(child, symbols)

    # helper function to look ahead one child
    def with_next(iterable):
        items, nexts = tee(iterable, 2)
        nexts = islice(nexts, 1, None)
        return zip_longest(items, nexts)

    if isinstance(node, UnparserRule):
        new_children = []
        for child, next_child in with_next(node.children):
            if (not next_child or
                next_child and isinstance(next_child, UnlexerRule) and next_child.name == 'DOT' or
                isinstance(child, UnlexerRule) and child.name == 'DOT'):
                new_children.append(child)
            else:
                new_children.extend([child, UnlexerRule(src=random.choice(symbols))])
        node.children = new_children

    return node
