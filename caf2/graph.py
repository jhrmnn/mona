# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import TypeVar, Deque, Set, Callable, Iterable, MutableSequence

_T = TypeVar('_T')


def traverse_execute(start: Iterable[_T],
                     parents: Callable[[_T, MutableSequence[_T]], Iterable[_T]],
                     execute: Callable[[_T], Iterable[_T]],
                     sentinel: Callable[[_T], bool] = None,
                     depth: bool = False,
                     eager_traverse: bool = False,
                     ) -> Set[_T]:
    visited: Set[_T] = set()
    traverse_queue = Deque[_T]()
    execution_queue = Deque[_T]()
    traverse_queue.extend(start)
    while traverse_queue or execution_queue:
        if (eager_traverse or not execution_queue) and traverse_queue:
            node = traverse_queue.pop() if depth else traverse_queue.popleft()
            visited.add(node)
            if sentinel and sentinel(node):
                continue
            for parent in parents(node, execution_queue):
                if parent not in visited:
                    traverse_queue.append(parent)
        else:
            node = execution_queue.popleft()
            traverse_queue.extend(execute(node))
    return visited


def traverse(start: Iterable[_T],
             parents: Callable[[_T], Iterable[_T]],
             sentinel: Callable[[_T], bool] = None,
             depth: bool = False,
             ) -> Set[_T]:
    return traverse_execute(
        start, (lambda n, _: parents(n)), (lambda n: ()), sentinel, depth,
        eager_traverse=True,
    )
