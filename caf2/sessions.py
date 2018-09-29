# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import warnings
from contextlib import contextmanager
from itertools import chain
from collections import defaultdict
import asyncio
from contextvars import ContextVar
from typing import Set, Any, Dict, Callable, Optional, \
    TypeVar, Iterator, NamedTuple, cast, Iterable, List, Tuple, \
    Union, Awaitable

from .hashing import Hash, Hashed, HashedCompositeLike
from .tasks import Task, HashedFuture, State, maybe_hashed
from .graph import traverse, traverse_async, NodeExecuted, \
    Action, Priority, default_priority
from .utils import Literal, split, Empty, Maybe, call_if
from .errors import SessionError, TaskError, FutureError, CafError

log = logging.getLogger(__name__)

_T = TypeVar('_T')
TaskExecute = Callable[[Task[Any], NodeExecuted[Task[Any]]], Awaitable[None]]

_active_session: ContextVar[Optional['Session']] = \
    ContextVar('active_session', default=None)


class SessionPlugin:
    name: str

    def __call__(self, sess: 'Session') -> None:
        sess.register_plugin(self.name, self)

    def post_enter(self, sess: 'Session') -> None:
        pass

    def pre_exit(self, sess: 'Session') -> None:
        pass

    def pre_run(self, sess: 'Session') -> None:
        pass

    def post_run(self, sess: 'Session') -> None:
        pass

    def wrap_execute(self, exe: TaskExecute) -> TaskExecute:
        return exe


class Graph(NamedTuple):
    deps: Dict[Hash, Set[Hash]]
    side_effects: Dict[Hash, Set[Hash]]
    backflow: Dict[Hash, Set[Hash]]


class Session:
    def __init__(self, plugins: Iterable[SessionPlugin] = None) -> None:
        self._tasks: Dict[Hash, Task[Any]] = {}
        self._objects: Dict[Hash, Hashed[Any]] = {}
        self._graph = Graph({}, defaultdict(set), {})
        self._running_task: ContextVar[Optional[Task[Any]]] = \
            ContextVar('running_task')
        self._running_task.set(None)
        self._storage: Dict[str, Any] = {}
        self._plugins: Dict[str, SessionPlugin] = {}
        for plugin in plugins or ():
            plugin(self)

    def _check_active(self) -> None:
        sess = _active_session.get()
        if sess is None or sess is not self:
            raise SessionError(f'Not active: {self!r}', self)

    @property
    def storage(self) -> Dict[str, Any]:
        self._check_active()
        return self._storage

    def register_plugin(self, name: str, plugin: SessionPlugin) -> None:
        self._plugins[name] = plugin

    def _run_plugins(self, func: str, reverse: bool = False) -> None:
        plugins: Iterable[SessionPlugin] = self._plugins.values()
        if reverse:
            plugins = reversed(list(plugins))
        for plugin in plugins:
            getattr(plugin, func)(self)

    def __enter__(self) -> 'Session':
        assert _active_session.get() is None
        self._active_session_token = _active_session.set(self)
        self._run_plugins('post_enter')
        return self

    def _filter_tasks(self, cond: Callable[[Task[Any]], bool]) -> List[Task[Any]]:
        return list(filter(cond, self._tasks.values()))

    def __exit__(self, exc_type: Any, *args: Any) -> None:
        self._run_plugins('pre_exit', reverse=True)
        assert _active_session.get() is self
        _active_session.reset(self._active_session_token)
        del self._active_session_token
        if exc_type is None:
            tasks_not_run = self._filter_tasks(lambda t: t.state < State.HAS_RUN)
            if tasks_not_run:
                warnings.warn(
                    f'tasks were never run: {tasks_not_run}', RuntimeWarning
                )
        self._tasks.clear()
        self._objects.clear()
        self._storage.clear()
        self._graph.deps.clear()
        self._graph.side_effects.clear()
        self._graph.backflow.clear()

    @property
    def running_task(self) -> Task[Any]:
        task = self._running_task.get()
        if task:
            return task
        raise SessionError(f'No running task: {self!r}', self)

    @contextmanager
    def _running_task_ctx(self, task: Task[Any]) -> Iterator[None]:
        assert not self._running_task.get()
        self._running_task.set(task)
        try:
            yield
        finally:
            assert self._running_task.get() is task
            self._running_task.set(None)

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
                raise TaskError(f'Not in session: {task!r}', task)
        if save:
            self._objects.update({o.hashid: o for o in objs})
        return tasks

    def create_task(self, corofunc: Callable[..., Awaitable[_T]], *args: Any,
                    label: str = None, default: Maybe[_T] = Empty._
                    ) -> Task[_T]:
        task = Task(corofunc, *args, label=label, default=default)
        parent_task = self._running_task.get()
        if parent_task:
            self._graph.side_effects[parent_task.hashid].add(task.hashid)
        try:
            return self._tasks[task.hashid]
        except KeyError:
            pass
        task.register()
        self._tasks[task.hashid] = task
        arg_tasks = self._process_objects(task.args, save=True)
        self._graph.deps[task.hashid] = set(t.hashid for t in arg_tasks)
        return task

    async def _run_task(self, task: Task[_T]) -> Union[_T, Hashed[_T]]:
        self._run_plugins('pre_run')
        result = await self.run_task_async(task)
        self._run_plugins('post_run')
        return result

    def run_task(self, task: Task[_T]) -> Union[_T, Hashed[_T]]:
        return asyncio.run(self._run_task(task))

    async def run_task_async(self, task: Task[_T]) -> Union[_T, Hashed[_T]]:
        if task.state < State.READY:
            raise TaskError(f'Not ready: {task!r}', task)
        if task.state > State.READY:
            raise TaskError(f'Task already run: {task!r}', task)
        with self._running_task_ctx(task):
            result = await task.corofunc(*(arg.value for arg in task.args))
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
                raise TaskError(f'{result!r} cannot be hooked', task)
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
        return hashed

    async def _execute(self, task: Task[Any], reg: NodeExecuted[Task[Any]]
                       ) -> None:
        await self.run_task_async(task)
        backflow = (
            self._tasks[h] for h in self._graph.backflow.get(task.hashid, ())
        )
        reg(backflow)

    def eval(self,
             obj: Any,
             depth: bool = False,
             priority: Priority = default_priority) -> Any:
        return asyncio.run(self.eval_async(obj, depth, priority))

    async def eval_async(self,
                         obj: Any,
                         depth: bool = False,
                         priority: Priority = default_priority) -> Any:
        fut = maybe_hashed(obj)
        if not isinstance(fut, HashedFuture):
            return obj
        self._run_plugins('pre_run')
        execute = self._execute
        for plugin in self._plugins.values():
            execute = plugin.wrap_execute(execute)  # type: ignore
        fut.register()
        async for action, task, progress in traverse_async(
                self._process_objects([fut], save=False),
                lambda task: (self._tasks[h] for h in chain(
                    self._graph.deps[task.hashid],
                    self._graph.backflow.get(task.hashid, ()),
                )),
                lambda task, reg: call_if(
                    task.state < State.HAS_RUN,
                    task.add_ready_callback, lambda t: reg(t)
                ),
                execute,
                lambda task: task.done(),
                depth,
                priority,
        ):
            tag = action.name
            if task:
                tag += f': {task}'
            log.debug(f'{tag}, progress: {progress}')
            if action is Action.EXECUTE:
                log.info(f'{task}: will run')
        log.info('Finished')
        self._run_plugins('post_run')
        try:
            return fut.value
        except FutureError as e:
            tasks_not_done = self._filter_tasks(lambda t: not t.done())
            if tasks_not_done:
                msg = f'Task dependency cycle: {tasks_not_done}'
                raise CafError(msg) from e
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
        session = _active_session.get()
        if session is None:
            raise CafError('No active session')
        return session
