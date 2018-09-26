# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import TypeVar, Deque, Set, Callable, Iterable, \
    MutableSequence, Dict, Generic, Tuple

_T = TypeVar('_T')
PopFunction = Callable[[MutableSequence[_T]], _T]


class MergedQueue(Generic[_T]):
    """
    Pops from the first nonempty given queue
    """
    def __init__(
            self, queues: Iterable[Tuple[MutableSequence[_T], PopFunction[_T]]]
    ) -> None:
        self._queues = list(queues)

    def __bool__(self) -> bool:
        return any(q for q, _ in self._queues)

    def pop(self) -> Tuple[_T, MutableSequence[_T]]:
        for queue, pop in self._queues:
            if queue:
                return pop(queue), queue
        else:
            raise IndexError('pop from empty MergedQueue')


def traverse(start: Iterable[_T],
             edge_from: Callable[[_T], Iterable[_T]],
             sentinel: Callable[[_T], bool] = None,
             register: Callable[[_T, MutableSequence[_T]], None] = None,
             execute: Callable[[_T], Iterable[_T]] = None,
             depth: bool = False,
             eager_execute: bool = False) -> Set[_T]:
    """
    Traverse a self-extending dynamic DAG and return visited nodes.

    :param start: Starting nodes
    :param edge_from: Returns nodes with incoming edge from the given node
    :param sentinal: Should traversal stop at the given node?
    :param register: Registers the given node for execution (not run on sentinels)
    :param execute: Executes the given node and returns new nodes with
                    incoming edge from it (run only on registered nodes)
    :param depth: Traverse depth-first if true, breadth-first otherwise
    :param eager_execute: Prioritize execution before traversal if true
    """
    execute = execute or (lambda n: ())
    visited: Set[_T] = set()
    traverse_queue, execution_queue = Deque[_T](), Deque[_T]()
    traverse_queue.extend(start)
    queues = [
        (traverse_queue, (lambda q: q.pop()) if depth else (lambda q: q.popleft()))
    ]
    if execute:
        queues.append((execution_queue, lambda q: q.popleft()))
        if eager_execute:
            queues.reverse()
    queue = MergedQueue(queues)
    while queue:
        n, q = queue.pop()
        if q is traverse_queue:
            visited.add(n)
            if sentinel and sentinel(n):
                continue
            if register:
                register(n, execution_queue)
            traverse_queue.extend(m for m in edge_from(n) if m not in visited)
        else:
            traverse_queue.extend(m for m in execute(n) if m not in visited)
    return visited


def traverse_id(start: Iterable[_T], edge_from: Callable[[_T], Iterable[_T]]
                ) -> Iterable[_T]:
    table: Dict[int, _T] = {}
    id_start = {id(x): x for x in start}
    table.update(id_start)

    def edge_from_id(n: int) -> Iterable[int]:
        xs = edge_from(table[n])
        update = {id(x): x for x in xs}
        table.update(update)
        return update.keys()

    visited_id = traverse(id_start.keys(), edge_from_id)
    return (table[n] for n in visited_id)
