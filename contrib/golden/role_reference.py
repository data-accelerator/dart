#!/usr/bin/env python3
"""Independent role-filtered placement and owner-rooted tree vectors (ADR-0009).

Uses the Python hash reference, never Go output. Reproduce the pinned fixture:
  python3 contrib/golden/role_reference.py > internal/engine/testdata/roles.json
The tree is materialized recursively, independently of Go's arithmetic descent.
"""
import json
import sys

sys.dont_write_bytecode = True
from golden_reference import rank


def parents(order, fanout):
    out = {}

    def visit(nodes, parent):
        if not nodes:
            return
        out[nodes[0]] = parent
        quotient, remainder = divmod(len(nodes) - 1, fanout)
        offset = 1
        for i in range(fanout):
            count = quotient + (i < remainder)
            visit(nodes[offset:offset + count], nodes[0])
            offset += count

    visit(order, "")
    return [out[node] for node in order]


def reference():
    # role: ordinary=0, root-only=1, non-root=2; state: Ready=1, Suspect=2.
    members = [dict(ID=name, Addr=name.lower()+":9000", Weight=weight,
                    Role=role, State=state)
               for name, weight, role, state in [
                   ("A", 1, 0, 1), ("B", 2, 1, 1), ("C", 4, 2, 1),
                   ("D", .5, 0, 1), ("E", 1.5, 1, 1), ("F", 2, 2, 1),
                   ("G", 1, 2, 1), ("H", 1, 0, 1), ("I", 16, 1, 2)]]
    cases = []
    for key in [0, 42, 7, 1024, 0xDEADBEEFCAFEBABE]:
        roots = rank(key, [(m["ID"], m["Weight"]) for m in members
                           if m["Role"] != 2 and m["State"] == 1])
        for readers in [None, [], ["C"], ["C", "D", "G", "D", "B", "unknown"],
                        ["A", "C", "D", "F", "G", "H", "B", "E", "I"]]:
            eligible = [(m["ID"], m["Weight"]) for m in members
                        if m["Role"] != 1 and m["State"] == 1
                        and (readers is None or m["ID"] in readers or m["ID"] == "C")]
            order = [roots[0]] + [node for node in rank(key, eligible) if node != roots[0]]
            cases.append(dict(key=key, readers=readers, owners=roots[:2],
                              roots=roots, order=order,
                              parents={str(k): parents(order, k) for k in [1, 2, 4]}))
    return dict(members=members, cases=cases)


if __name__ == "__main__":
    print(json.dumps(reference(), indent=2))
