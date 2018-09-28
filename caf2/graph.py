# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import TypeVar, Deque, Set, Callable, Iterable, \
    MutableSequence, Dict, Generic, Tuple, Awaitable

_T = TypeVar('_T')
PopFunction = Callable[[MutableSequence[_T]], _T]
NodeRegister = Callable[[_T, Callable[[Iterable[_T]], None]], None]
NodeExecuted = Callable[[_T, Iterable[_T]], None]
NodeExecutor = Callable[[_T, NodeExecuted[_T]], Awaitable[None]]


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


async def traverse_exec(start: Iterable[_T],
                        edges_from: Callable[[_T], Iterable[_T]],
                        register: NodeRegister[_T],
                        execute: NodeExecutor[_T],
                        sentinel: Callable[[_T], bool] = None,
                        depth: bool = False,
                        eager_execute: bool = False) -> Set[_T]:
    """
    Traverse a self-extending dynamic DAG and return visited nodes.

    :param start: Starting nodes
    :param edges_from: Returns nodes with incoming edge from the given node
    :param sentinal: Should traversal stop at the given node?
    :param register: Registers the given node for execution (not run on sentinels)
    :param execute: Executes the given node and announces execution and
                    new generated nodes with incoming edge from it
                    (run only on registered nodes)
    :param depth: Traverse depth-first if true, breadth-first otherwise
    :param eager_execute: Prioritize execution before traversal if true
    """
    visited: Set[_T] = set()
    pending: Set[_T] = set()
    traverse_queue, execute_queue = Deque[_T](), Deque[_T]()

    def executed(n: _T, ms: Iterable[_T]) -> None:
        pending.remove(n)
        traverse_queue.extend(m for m in ms if m not in visited)

    traverse_queue.extend(start)
    queues = [
        (traverse_queue, (lambda q: q.pop()) if depth else (lambda q: q.popleft()))
    ]
    if execute:
        queues.append((execute_queue, lambda q: q.popleft()))
        if eager_execute:
            queues.reverse()
    queue = MergedQueue(queues)
    while queue or pending:
        n, q = queue.pop()
        if q is traverse_queue:
            visited.add(n)
            if sentinel and sentinel(n):
                continue
            register(n, lambda ms: execute_queue.extend(ms))
            traverse_queue.extend(m for m in edges_from(n) if m not in visited)
        else:
            pending.add(n)
            await execute(n, executed)
    return visited


def traverse(start: Iterable[_T],
             edges_from: Callable[[_T], Iterable[_T]],
             sentinel: Callable[[_T], bool] = None,
             depth: bool = False) -> Set[_T]:
    visited: Set[_T] = set()
    queue = Deque[_T]()
    queue.extend(start)
    while queue:
        n = queue.pop() if depth else queue.popleft()
        visited.add(n)
        if sentinel and sentinel(n):
            continue
        queue.extend(m for m in edges_from(n) if m not in visited)
    return visited


def traverse_id(start: Iterable[_T], edges_from: Callable[[_T], Iterable[_T]]
                ) -> Iterable[_T]:
    table: Dict[int, _T] = {}
    id_start = {id(x): x for x in start}
    table.update(id_start)

    def edges_from_id(n: int) -> Iterable[int]:
        xs = edges_from(table[n])
        update = {id(x): x for x in xs}
        table.update(update)
        return update.keys()

    visited_id = traverse(id_start.keys(), edges_from_id)
    return (table[n] for n in visited_id)
