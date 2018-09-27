# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import warnings
from contextlib import contextmanager
from itertools import chain
from collections import defaultdict
from typing import Set, Any, Dict, Callable, Optional, \
    TypeVar, Iterator, NamedTuple, cast, Iterable, List, Tuple

from .hashing import Hash, Hashed, HashedCompositeLike
from .tasks import Task, HashedFuture, State, maybe_hashed, FutureNotDone
from .graph import traverse, NodeExecuted
from .utils import Literal, split, Empty, Maybe, call_if
from .errors import ArgNotInSession, DependencyCycle, NoActiveSession, \
    UnhookableResult

log = logging.getLogger(__name__)

_T = TypeVar('_T')


class Graph(NamedTuple):
    deps: Dict[Hash, Set[Hash]]
    side_effects: Dict[Hash, Set[Hash]]
    backflow: Dict[Hash, Set[Hash]]


class Session:
    _active: Optional['Session'] = None

    def __init__(self) -> None:
        self._tasks: Dict[Hash, Task[Any]] = {}
        self._objects: Dict[Hash, Hashed[Any]] = {}
        self._graph = Graph({}, defaultdict(set), {})
        self._parent_task: Optional[Task[Any]] = None
        self.storage: Dict[str, Any] = {}

    def __enter__(self) -> 'Session':
        assert Session._active is None
        Session._active = self
        return self

    def _filter_tasks(self, cond: Callable[[Task[Any]], bool]) -> List[Task[Any]]:
        return list(filter(cond, self._tasks.values()))

    def __exit__(self, exc_type: Any, *args: Any) -> None:
        Session._active = None
        if exc_type is None:
            tasks_not_run = self._filter_tasks(lambda t: t.state < State.HAS_RUN)
            if tasks_not_run:
                warnings.warn(
                    f'tasks were never run: {tasks_not_run}', RuntimeWarning
                )
        self._tasks.clear()
        self._objects.clear()
        self.storage.clear()
        self._graph.deps.clear()
        self._graph.side_effects.clear()
        self._graph.backflow.clear()

    @contextmanager
    def record(self, task: Task[Any]) -> Iterator[None]:
        assert not self._parent_task
        self._parent_task = task
        try:
            yield
        finally:
            self._parent_task = None

    def _process_objects(self, objs: Iterable[Hashed[Any]], *, save: bool
                         ) -> List[Task[Any]]:
        objs = list(traverse(
            objs,
            lambda o: (
                o.components
                if isinstance(o, HashedCompositeLike)
                else cast(Iterable[Hashed[Any]], o.parents)
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
            if task.hashid not in self._tasks:
                raise ArgNotInSession(repr(task))
        if save:
            self._objects.update({o.hashid: o for o in objs})
        return tasks

    def create_task(self, func: Callable[..., _T], *args: Any,
                    label: str = None, default: Maybe[_T] = Empty._
                    ) -> Task[_T]:
        task = Task(func, *args, label=label, default=default)
        if self._parent_task:
            self._graph.side_effects[self._parent_task.hashid].add(task.hashid)
        try:
            return self._tasks[task.hashid]
        except KeyError:
            pass
        task.register()
        self._tasks[task.hashid] = task
        arg_tasks = self._process_objects(task.args, save=True)
        self._graph.deps[task.hashid] = set(t.hashid for t in arg_tasks)
        return task

    def run_task(self, task: Task[_T]) -> Optional[_T]:
        assert task.state is State.READY
        log.info(f'{task}: will run')
        with self.record(task):
            result = task.func(*(arg.value for arg in task.args))
        side_effects = [
            self._tasks[h] for h in self._graph.side_effects[task.hashid]
        ]
        if side_effects:
            log.debug(
                f'{task}: created tasks: {list(map(Literal, side_effects))}'
            )
        hashed = maybe_hashed(result)
        if hashed is None:
            if task.has_hook():
                raise UnhookableResult(f'{result!r} of {task}')
            task.set_result(result)
            return result
        if task.has_hook():
            hashed = task.run_hook(hashed)
        if not isinstance(hashed, HashedFuture):
            task.set_result(hashed)
        else:
            fut = hashed
            if fut.done():
                task.set_result(fut.value)
            else:
                log.debug(f'{task}: has run, pending: {fut}')
                task.set_future_result(fut)
                fut.add_done_callback(lambda fut: task.set_result(fut.value))
                fut.register()
        backflow = self._process_objects([hashed], save=True)
        self._graph.backflow[task.hashid] = set(t.hashid for t in backflow)
        return None

    def _execute(self, task: Task[Any], reg: NodeExecuted[Task[Any]]) -> None:
        self.run_task(task)
        reg(task, (
            self._tasks[h] for h in self._graph.backflow.get(task.hashid, ())
        ))

    def eval(self, obj: Any, depth: bool = False, eager_traverse: bool = False
             ) -> Any:
        fut = maybe_hashed(obj)
        if not isinstance(fut, HashedFuture):
            return obj
        fut.register()
        traverse(
            self._process_objects([fut], save=False),
            lambda task: (self._tasks[h] for h in chain(
                self._graph.deps[task.hashid],
                self._graph.backflow.get(task.hashid, ()),
            )),
            lambda task: task.done(),
            lambda task, reg: call_if(
                task.state < State.HAS_RUN,
                task.add_ready_callback, lambda t: reg((t,))
            ),
            self._execute,
            depth,
            eager_traverse,
        )

        try:
            return fut.value
        except FutureNotDone as e:
            tasks_not_done = self._filter_tasks(lambda t: not t.done())
            if tasks_not_done:
                raise DependencyCycle(tasks_not_done) from e
            raise

    def dot_graph(self, *args: Any, **kwargs: Any) -> Any:
        from graphviz import Digraph  # type: ignore

        dot = Digraph(*args, **kwargs)
        for child, parents in self._graph.deps.items():
            dot.node(child, repr(Literal(self._tasks[child])))
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
