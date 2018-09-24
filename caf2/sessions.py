# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import warnings
from contextlib import contextmanager
from typing import Set, Any, Dict, Callable, Optional, List, Deque, \
    TypeVar, Iterator, NamedTuple

from .futures import Future, CafError, FutureNotDone
from .hashing import Hash
from .tasks import Task, HashedFuture, State, maybe_future
from .collections import HashedDeque
from .utils import Literal

log = logging.getLogger(__name__)

_T = TypeVar('_T')


def extract_tasks(fut: Future[Any], shallow: bool = False) -> Set[Task[Any]]:
    tasks: Set[Task[Any]] = set()
    visited: Set[Future[Any]] = set()
    queue = Deque[Future[Any]]()
    queue.append(fut)
    while queue:
        fut = queue.popleft()
        visited.add(fut)
        if isinstance(fut, Task):
            tasks.add(fut)
            if shallow:
                continue
        for parent in fut.pending:
            if parent not in visited:
                queue.append(parent)
    return tasks


class NoActiveSession(CafError):
    pass


class ArgNotInSession(CafError):
    pass


class DependencyCycle(CafError):
    pass


class Graph(NamedTuple):
    deps: Dict[Hash, Set[Hash]]
    side_effects: Dict[Hash, Set[Hash]]
    backflow: Dict[Hash, Set[Hash]]


class Session:
    _active: Optional['Session'] = None

    def __init__(self) -> None:
        self._tasks: Dict[Hash, Task[Any]] = {}
        self._graph = Graph({}, {}, {})
        self._task_tape: Optional[List[Task[Any]]] = None

    def __enter__(self) -> 'Session':
        assert Session._active is None
        Session._active = self
        return self

    def __exit__(self, exc_type: Any, *args: Any) -> None:
        Session._active = None
        if exc_type is None:
            tasks_not_run = [
                task for task in self._tasks.values()
                if task.state < State.HAS_RUN
            ]
            if tasks_not_run:
                warnings.warn(
                    f'tasks were never run: {tasks_not_run}', RuntimeWarning
                )
        self._tasks.clear()
        self._graph = Graph({}, {}, {})

    def __contains__(self, task: Task[Any]) -> bool:
        return task.hashid in self._tasks

    @contextmanager
    def record(self, tape: List[Task[Any]]) -> Iterator[None]:
        self._task_tape = tape
        try:
            yield
        finally:
            self._task_tape = None

    def create_task(self, func: Callable[..., _T], *args: Any, **kwargs: Any
                    ) -> Task[_T]:
        task = Task(func, *args, **kwargs)
        try:
            task = self._tasks[task.hashid]
        except KeyError:
            pass
        else:
            return task
        finally:
            if self._task_tape is not None:
                self._task_tape.append(task)
        parents: Set[Hash] = set()
        for arg in task.pending:  # TODO this should be all args, even finished futures
            if isinstance(arg, Task):
                parents.add(arg.hashid)
                if arg not in self:
                    raise ArgNotInSession(repr(arg))
            else:
                for arg_task in extract_tasks(arg):
                    parents.add(arg_task.hashid)
                    if arg_task not in self:
                        raise ArgNotInSession(f'{arg!r} -> {arg_task!r}')
        task.register()
        self._tasks[task.hashid] = task
        self._graph.deps[task.hashid] = parents
        return task

    def run_task(self, task: Task[_T], check_ready: bool = True
                 ) -> Optional[_T]:
        assert not task.done()
        if check_ready:
            assert task.state > State.PENDING
        args = [
            arg.result(check_done=check_ready)
            if isinstance(arg, HashedFuture)
            else arg.value
            for arg in task.args
        ]
        with self.record(task.side_effects):
            result = task.func(*args)
        if task.side_effects:
            self._graph.side_effects[task.hashid] = \
                set(created_task.hashid for created_task in task.side_effects)
            log.debug(f'{task}: created children: {list(map(Literal, task.side_effects))}')
        if task.state is State.PENDING:
            return result
        fut = maybe_future(result)
        if fut:
            if fut.done():
                task.set_result(fut.result())
            else:
                log.debug(f'{task}: has run, pending: {fut}')
                task.set_future_result(fut)
                fut.add_done_callback(lambda fut: task.set_result(fut.result()))
                fut.register()
        else:
            task.set_result(result)
        return None

    def eval(self, obj: Any) -> Any:
        fut = maybe_future(obj)
        if not fut:
            return obj
        fut.register()
        queue = HashedDeque[Task[Any]]()

        def schedule(task: Task[Any]) -> None:
            if task.state < State.HAS_RUN and task not in queue:
                queue.append(task)

        def process_future(fut: HashedFuture[Any]) -> None:
            for task in extract_tasks(fut):
                if task.state < State.HAS_RUN:
                    task.add_ready_callback(schedule)

        process_future(fut)
        while queue:
            task = queue.popleft()
            if task.state > State.READY:
                continue
            log.info(f'{task}: will run')
            self.run_task(task)
            if not task.done():
                process_future(task.future_result())
                self._graph.backflow[task.hashid] = set(
                    t.hashid for t in extract_tasks(
                        task.future_result(), shallow=True
                    )
                )
        try:
            return fut.result()
        except FutureNotDone as e:
            raise DependencyCycle(
                [task for task in self._tasks.values() if not task.done()]
            ) from e

    def dot_graph(self, *args: Any, **kwargs: Any) -> Any:
        from graphviz import Digraph  # type: ignore

        dot = Digraph(*args, **kwargs)
        for child, parents in self._graph.deps.items():
            dot.node(child, str(self._tasks[child]))
            for parent in parents:
                dot.edge(child, parent)
        for origin, tasks in self._graph.side_effects.items():
            for task in tasks:
                dot.edge(origin, task, style='dotted')
        for target, tasks in self._graph.backflow.items():
            for task in tasks:
                dot.edge(
                    task, target,
                    style='tapered', penwidth='7', dir='back', arrowtail='none'
                )
        return dot

    @classmethod
    def active(cls) -> 'Session':
        if cls._active is None:
            raise NoActiveSession()
        return cls._active
