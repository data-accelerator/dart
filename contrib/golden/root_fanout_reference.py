#!/usr/bin/env python3
"""Independent explicit-tree reference for asymmetric preorder fanout.

Construct child lists recursively from contiguous slices, then traverse the
materialized tree for depths. No Go code or arithmetic parent lookup is used.
"""
import json


def tree(n, k, root_k):
    parents = [-1] * n
    children = [[] for _ in range(n)]

    def build(nodes, degree):
        root, rest = nodes[0], nodes[1:]
        sizes = [len(rest) // degree + (i < len(rest) % degree)
                 for i in range(degree)]
        offset = 0
        for size in sizes:
            part = rest[offset:offset + size]
            offset += size
            if part:
                parents[part[0]] = root
                children[root].append(part[0])
                build(part, k)

    build(list(range(n)), root_k)
    depths = [0] * n
    for node in range(1, n):
        depths[node] = depths[parents[node]] + 1
    return dict(n=n, k=k, root_k=root_k, parents=parents,
                children=children, depths=depths)


CASES = [(1, 2, 1), (2, 4, 1), (8, 2, 1), (8, 2, 2), (12, 2, 3),
         (16, 4, 1), (11, 1, 3), (5, 2, 8), (9, 8, 2), (8, 1, 1)]
print(json.dumps([tree(*case) for case in CASES], indent=2))
