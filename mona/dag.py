# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Container,
    Deque,
    Dict,
    Iterable,
    Iterator,
    MutableSequence,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

__all__ = ['traverse_async', 'traverse', 'traverse_id']

_T = TypeVar('_T')
NodeScheduler = Callable[[_T, Callable[[_T], None]], None]
NodeResult = Tuple[_T, Optional[Exception], Iterable[_T]]
NodeExecuted = Callable[[NodeResult[_T]], None]
NodeExecutor = Callable[[_T, NodeExecuted[_T]], Awaitable[None]]
Priority = Tuple['Action', 'Action', 'Action']


def extend_from(
    src: Iterable[_T], seq: MutableSequence[_T], *, filter: Container[_T]
) -> None:
    seq.extend(x for x in src if x not in filter)


class Action(Enum):
    RESULTS = 0
    EXECUTE = 1
    TRAVERSE = 2

    def __repr__(self) -> str:
        return self.name


class Step(NamedTuple):
    action: Action
    node: Optional[Any]  # should be _T
    progress: Dict[str, int]


class NodeException(NamedTuple):
    node: Any  # should be _T
    exc: Exception


default_priority = cast(Priority, tuple(Action))


# only limited override for use in traverse_async()
class SetDeque(Deque[_T]):
    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self._set: Set[_T] = set()

    def append(self, x: _T) -> None:
        if x not in self._set:
            self._set.add(x)
            super().append(x)

    def extend(self, xs: Iterable[_T]) -> None:
        for x in xs:
            self.append(x)

    def pop(self) -> _T:  # type: ignore
        x = super().pop()
        self._set.remove(x)
        return x

    def popleft(self) -> _T:
        x = super().popleft()
        self._set.remove(x)
        return x


async def traverse_async(
    start: Iterable[_T],
    edges_from: Callable[[_T], Iterable[_T]],
    schedule: NodeScheduler[_T],
    execute: NodeExecutor[_T],
    depth: bool = False,
    priority: Priority = default_priority,
) -> AsyncGenerator[Union[Step, NodeException], bool]:
    """Asynchronous generator that traverses a self-extending DAG.

    Yields potentials steps, and receives whether those should be taken.

    :param start: Starting nodes
    :param edges_from: Returns nodes with incoming edge from the given node
    :param schedule: Schedule the given node for execution
    :param execute: Execute the given node and return new generated nodes
                    with incoming edge from it (run only on scheduled nodes)
    :param depth: Traverse depth-first if true, breadth-first otherwise
    :param priority: Priorize steps in order
    """
    visited: Set[_T] = set()
    to_visit, to_execute = SetDeque[_T](), Deque[_T]()
    done: 'asyncio.Queue[NodeResult[_T]]' = asyncio.Queue()
    executing, executed = 0, 0
    actionable: Dict[Action, Callable[[], bool]] = {
        Action.RESULTS: lambda: not done.empty(),
        Action.EXECUTE: lambda: bool(to_execute),
        Action.TRAVERSE: lambda: bool(to_visit),
    }
    to_visit.extend(start)
    while True:
        for action in priority:
            if actionable[action]():
                break
        else:
            if executing == 0:
                break
            action = Action.RESULTS
        progress = {
            'executing': executing - done.qsize(),
            'to_execute': len(to_execute),
            'to_visit': len(to_visit),
            'with_result': done.qsize(),
            'done': executed,
            'visited': len(visited),
        }
        if action is Action.TRAVERSE:
            node = to_visit.pop() if depth else to_visit.popleft()
            visited.add(node)
            if not (yield Step(action, node, progress)):
                continue
            schedule(node, to_execute.append)
            extend_from(edges_from(node), to_visit, filter=visited)
        elif action is Action.RESULTS:
            yield Step(action, None, progress)
            node, exc, nodes = await done.get()
            if exc:
                yield NodeException(node, exc)
            extend_from(nodes, to_visit, filter=visited)
            executing -= 1
            executed += 1
        else:
            assert action is Action.EXECUTE
            node = to_execute.popleft()
            if not (yield Step(action, node, progress)):
                continue
            executing += 1
            try:
                await execute(node, done.put_nowait)
            except Exception as exc:
                yield NodeException(node, exc)


def traverse(
    start: Iterable[_T],
    edges_from: Callable[[_T], Iterable[_T]],
    sentinel: Callable[[_T], bool] = None,
    depth: bool = False,
) -> Iterator[_T]:
    """Traverse a DAG, yield visited notes."""
    visited: Set[_T] = set()
    queue = Deque[_T]()
    queue.extend(start)
    while queue:
        n = queue.pop() if depth else queue.popleft()
        visited.add(n)
        yield n
        if sentinel and sentinel(n):
            continue
        queue.extend(m for m in edges_from(n) if m not in visited)


def traverse_id(
    start: Iterable[_T], edges_from: Callable[[_T], Iterable[_T]]
) -> Iterable[_T]:
    """Traverse a DAG, yield visited notes.

    Nodes are stored by their ids, not hashes.
    """
    table: Dict[int, _T] = {}

    def ids_from(ns: Iterable[_T]) -> Iterable[int]:
        update = {id(n): n for n in ns}
        table.update(update)
        return update.keys()

    for n in traverse(ids_from(start), lambda n: ids_from(edges_from(table[n]))):
        yield table[n]
