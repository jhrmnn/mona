# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import warnings
from contextlib import contextmanager
from itertools import chain
from typing import Set, Any, Dict, Callable, Optional, \
    TypeVar, Iterator, NamedTuple, cast, Iterable, List, Tuple

from .futures import Future, CafError, FutureNotDone
from .hashing import Hash, Hashed
from .tasks import Task, HashedFuture, State, maybe_future
from .collections import HashedDeque, traverse
from .utils import Literal, split

log = logging.getLogger(__name__)

_T = TypeVar('_T')


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
        self._objects: Dict[Hash, Hashed[Any]] = {}
        self._graph = Graph({}, {}, {})
        self._task_tape: Optional[Callable[[Task[Any]], None]] = None

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
        self._objects.clear()
        self._graph = Graph({}, {}, {})

    def __contains__(self, task: Task[Any]) -> bool:
        return task.hashid in self._tasks

    @contextmanager
    def record(self, tape: Callable[[Task[Any]], None]) -> Iterator[None]:
        self._task_tape = tape
        try:
            yield
        finally:
            self._task_tape = None

    def _process_objects(self, objs: Iterable[Hashed[Any]], save: bool = True
                         ) -> List[Task[Any]]:
        objs = list(traverse(
            objs,
            lambda o: (
                cast(Iterable[Hashed[Any]], o.parents)
                if isinstance(o, HashedFuture)
                else []
            ),
            lambda o: isinstance(o, Task)
        ))
        tasks, objs = cast(
            Tuple[List[Task[Any]], List[Hashed[Any]]],
            split(objs, lambda o: isinstance(o, Task))
        )
        for task in tasks:
            if task not in self:
                raise ArgNotInSession(repr(task))
        if save:
            self._objects.update({o.hashid: o for o in objs})
        return tasks

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
                self._task_tape(task)
        task.register()
        self._tasks[task.hashid] = task
        tasks = self._process_objects(task.args)
        self._graph.deps[task.hashid] = set(t.hashid for t in tasks)
        return task

    def run_task(self, task: Task[_T]) -> None:
        assert task.state is State.READY
        args = [
            arg.result() if isinstance(arg, HashedFuture) else arg.value
            for arg in task.args
        ]
        with self.record(task.add_side_effect):
            result = task.func(*args)
        if task.side_effects:
            self._graph.side_effects[task.hashid] = \
                set(created_task.hashid for created_task in task.side_effects)
            log.debug(
                f'{task}: created children: '
                f'{list(map(Literal, task.side_effects))}'
            )
        fut = maybe_future(result)
        if isinstance(fut, bytes):
            task.set_result(result)
        else:
            if fut.done():
                task.set_result(fut.result())
            else:
                log.debug(f'{task}: has run, pending: {fut}')
                task.set_future_result(fut)
                fut.add_done_callback(lambda fut: task.set_result(fut.result()))
                fut.register()

    def eval(self, obj: Any) -> Any:
        fut = maybe_future(obj)
        if isinstance(fut, bytes):
            return obj
        fut.register()
        queue = HashedDeque[Task[Any]]()

        def schedule(task: Task[Any]) -> None:
            if task.state < State.HAS_RUN and task not in queue:
                queue.append(task)

        def process_future(fut: HashedFuture[Any], *, save: bool
                           ) -> List[Task[Any]]:
            tasks = self._process_objects([fut], save)
            unqueued_tasks = chain.from_iterable(traverse(
                [task],
                lambda t: (self._tasks[h] for h in self._graph.deps[t.hashid]),
                lambda t: t in queue,
                False
            ) for task in tasks)
            for task in unqueued_tasks:
                if task.state < State.HAS_RUN:
                    task.add_ready_callback(schedule)
            return tasks

        process_future(fut, save=False)
        while queue:
            task = queue.popleft()
            if task.state > State.READY:
                continue
            log.info(f'{task}: will run')
            self.run_task(task)
            if not task.done():
                backflow = process_future(task.future_result(), save=True)
                self._graph.backflow[task.hashid] = \
                    set(t.hashid for t in backflow)
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
