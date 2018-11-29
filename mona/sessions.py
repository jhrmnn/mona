# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import logging
import warnings
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from functools import wraps
from itertools import chain
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from .dag import (
    Action,
    NodeException,
    NodeExecuted,
    Priority,
    default_priority,
    traverse,
    traverse_async,
)
from .errors import FutureError, MonaError, SessionError, TaskError
from .futures import STATE_COLORS
from .hashing import Hash, Hashed
from .pluggable import Pluggable, Plugin
from .tasks import Corofunc, HashedFuture, State, Task, TaskComposite
from .utils import Literal, call_if, split

__version__ = '0.1.0'
__all__ = ['Session']

log = logging.getLogger(__name__)

_T = TypeVar('_T')
ATask = Task[object]
TaskExecute = Callable[[ATask, NodeExecuted[ATask]], Awaitable[None]]
ExceptionHandler = Callable[[ATask, Exception], bool]
TaskFilter = Callable[[ATask], bool]

_active_session: ContextVar[Optional['Session']] = ContextVar(
    'active_session', default=None
)


class SessionPlugin(Plugin['Session']):
    def post_enter(self, sess: 'Session') -> None:
        pass

    def pre_exit(self, sess: 'Session') -> None:
        pass

    async def pre_run(self) -> None:
        pass

    async def post_run(self) -> None:
        pass

    def post_task_run(self, task: ATask) -> None:
        pass

    def save_hashed(self, objs: Sequence[Hashed[object]]) -> None:
        pass

    def ignored_exception(self) -> None:
        pass

    def wrap_execute(self, exe: TaskExecute) -> TaskExecute:
        return exe

    def post_create(self, task: ATask) -> None:
        pass


class SessionGraph(NamedTuple):
    deps: Dict[Hash, FrozenSet[Hash]]
    side_effects: Dict[Hash, List[Hash]]
    backflow: Dict[Hash, FrozenSet[Hash]]


class Session(Pluggable):
    """A context manager in which tasks can be created.

    :param plugins: session plugins to load. This is equivalent to calling each
                    plugin with the created session as an argument
    :param bool warn: warn at the end of session if some created tasks were not
                 executed
    """

    def __init__(
        self, plugins: Iterable[SessionPlugin] = None, warn: bool = True
    ) -> None:
        Pluggable.__init__(self)
        for plugin in plugins or ():
            plugin(self)
        self._tasks: Dict[Hash, ATask] = {}
        self._graph = SessionGraph({}, defaultdict(list), {})
        self._running_task: ContextVar[Optional[ATask]] = ContextVar('running_task')
        self._running_task.set(None)
        self._storage: Dict[str, Any] = {}
        self._warn = warn
        self._skipped = False

    def _check_active(self) -> None:
        sess = _active_session.get()
        if sess is None or sess is not self:
            raise SessionError(f'Not active: {self!r}', self)

    @property
    def storage(self) -> Dict[str, object]:
        """General-purpose dictionary-based storage."""
        self._check_active()
        return self._storage

    def get_side_effects(self, task: ATask) -> Iterable[ATask]:
        """Return tasks created by a given task."""
        return tuple(self._tasks[h] for h in self._graph.side_effects[task.hashid])

    def all_tasks(self) -> Iterable[ATask]:
        """Return all tasks created in session."""
        yield from self._tasks.values()

    def __enter__(self) -> 'Session':
        assert _active_session.get() is None
        self._active_session_token = _active_session.set(self)
        self.run_plugins('post_enter', self)
        return self

    def _filter_tasks(self, cond: TaskFilter) -> List[ATask]:
        return list(filter(cond, self._tasks.values()))

    def __exit__(self, exc_type: Any, *args: Any) -> None:
        assert _active_session.get() is self
        self.run_plugins('pre_exit', self)
        _active_session.reset(self._active_session_token)
        del self._active_session_token
        if self._warn and not self._skipped and exc_type is None:
            tasks_not_run = self._filter_tasks(lambda t: t.state < State.RUNNING)
            if tasks_not_run:
                warnings.warn(f'tasks have never run: {tasks_not_run}', RuntimeWarning)
        self._tasks.clear()
        self._storage.clear()
        self._graph.deps.clear()
        self._graph.side_effects.clear()
        self._graph.backflow.clear()

    @property
    def running_task(self) -> ATask:  # noqa: D401
        """Currently running task."""
        task = self._running_task.get()
        if task:
            return task
        raise SessionError(f'No running task: {self!r}', self)

    @contextmanager
    def _running_task_ctx(self, task: ATask) -> Iterator[None]:
        assert not self._running_task.get()
        self._running_task.set(task)
        try:
            yield
        finally:
            assert self._running_task.get() is task
            self._running_task.set(None)

    def _process_objects(self, objs: Iterable[Hashed[object]]) -> List[ATask]:
        objs = list(
            traverse(objs, lambda o: o.components, lambda o: isinstance(o, Task))
        )
        tasks, objs = cast(
            Tuple[List[ATask], List[Hashed[object]]],
            split(objs, lambda o: isinstance(o, Task)),
        )
        for task in tasks:
            if task.hashid not in self._tasks:
                raise TaskError(f'Not in session: {task!r}', task)
        self.run_plugins('save_hashed', objs)
        return tasks

    def register_task(self, task: Task[_T]) -> Tuple[Task[_T], bool]:
        """Register a task in a session."""
        try:
            return cast(Task[_T], self._tasks[task.hashid]), False
        except KeyError:
            pass
        self._tasks[task.hashid] = task
        task.register()
        arg_tasks = self._process_objects(task.args)
        self._graph.deps[task.hashid] = frozenset(t.hashid for t in arg_tasks)
        return task, True

    def add_side_effect_of(self, caller: ATask, callee: ATask) -> None:
        """Register a task created by a task."""
        self._graph.side_effects[caller.hashid].append(callee.hashid)

    def create_task(
        self, corofunc: Corofunc[_T], *args: Any, **kwargs: Any
    ) -> Task[_T]:
        """Create a new task.

        :param corofunc: a coroutine function to be executed
        :param args: arguments to the coroutine
        :param kwargs: keyword arguments passed to :class:`~tasks.Task`
        """
        task = Task(corofunc, *args, **kwargs)
        caller = self._running_task.get()
        if caller:
            self.add_side_effect_of(caller, task)
        task, registered = self.register_task(task)
        if registered:
            self.run_plugins('post_create', task)
        return task

    @asynccontextmanager
    async def run_context(self) -> AsyncGenerator[None, None]:
        """Context in which tasks should be run."""
        await self.run_plugins_async('pre_run')
        try:
            yield
        finally:
            await self.run_plugins_async('post_run')

    async def _run_task(self, task: Task[_T]) -> Union[_T, Hashed[_T]]:
        async with self.run_context():
            return await self.run_task_async(task)

    def run_task(self, task: Task[_T]) -> Union[_T, Hashed[_T]]:
        """Run a task.

        :param task: task to run

        Return the result of the task's coroutine function or it's hashed
        instance if hashable.
        """
        return asyncio.run(self._run_task(task))

    def set_result(self, task: Task[_T], result: Union[_T, Hashed[_T]]) -> None:
        """Attach a result to a task."""
        if not isinstance(result, Hashed):
            task.set_result(result)
            return
        if not isinstance(result, HashedFuture) or result.done():
            task.set_result(result)
        else:
            log.debug(f'{task}: has run, pending: {result}')
            task.set_future_result(result)
            result.add_done_callback(lambda fut: task.set_done())
            result.register()
        backflow = self._process_objects([result])
        self._graph.backflow[task.hashid] = frozenset(t.hashid for t in backflow)

    async def run_task_async(self, task: Task[_T]) -> Union[_T, Hashed[_T]]:
        """Run a task asynchronously."""
        if task.state < State.READY:
            raise TaskError(f'Not ready: {task!r}', task)
        if task.state > State.READY:
            raise TaskError(f'Task was already run: {task!r}', task)
        task.set_running()
        with self._running_task_ctx(task):
            raw_result = await task.corofunc(*(arg.value for arg in task.args))
        task.set_has_run()
        side_effects = self.get_side_effects(task)
        if side_effects:
            log.debug(f'{task}: created tasks: {list(map(Literal, side_effects))}')
        result = cast(_T, TaskComposite.maybe_hashed(raw_result)) or raw_result
        self.set_result(task, result)
        self.run_plugins('post_task_run', task)
        return result

    async def _traverse_execute(self, task: ATask, done: NodeExecuted[ATask]) -> None:
        await self.run_task_async(task)
        backflow = (self._tasks[h] for h in self._graph.backflow.get(task.hashid, ()))
        done((task, None, backflow))

    def eval(self, *args: Any, **kwargs: Any) -> Any:
        """Blocking version of :meth:`eval_async`."""
        return asyncio.run(self.eval_async(*args, **kwargs))

    # TODO reduce complexity
    async def _eval_async(  # noqa: C901
        self,
        obj: object,
        depth: bool = False,
        priority: Priority = default_priority,
        exception_handler: ExceptionHandler = None,
        task_filter: TaskFilter = None,
        limit: int = None,
    ) -> Any:
        """Evaluate an object by running all tasks it references.

        This includes all newly created tasks that are referenced indirectly.

        :param obj: any hashable object
        :param bool depth: traverse DAG depth-first if true, breadth-first otherwise
        :param tuple priority: prioritize steps in DAG traversal in order
        :param exception_handler: callable that accepts a task and an exception
                                  it raised and returns True if the exception
                                  should be ignored
        :param task_filter: callable that accepts a task and returns True if
                            the task should be executed
        :param int limit: limit of the number of executed task

        Return the evaluated object.
        """
        fut = TaskComposite.maybe_hashed(obj)
        if not isinstance(fut, HashedFuture):
            return obj
        fut.register()
        exceptions = {}
        traversal = traverse_async(
            self._process_objects([fut]),
            lambda task: (
                self._tasks[h]
                for h in chain(
                    self._graph.deps[task.hashid],
                    self._graph.backflow.get(task.hashid, ()),
                )
            ),
            lambda task, reg: call_if(
                task.state < State.RUNNING, task.add_ready_callback, lambda t: reg(t)
            ),
            self.run_plugins('wrap_execute', self._traverse_execute, wrap_first=True),
            depth,
            priority,
        )
        n_executed = 0
        shutdown = False
        do_step: bool = None  # type: ignore
        while True:
            try:
                step_or_exception = await traversal.asend(do_step)
            except StopAsyncIteration:
                break
            if isinstance(step_or_exception, NodeException):
                task, exc = step_or_exception
                if isinstance(exc, (MonaError, AssertionError, asyncio.CancelledError)):
                    raise exc
                else:
                    assert isinstance(exc, Exception)
                    if exception_handler and exception_handler(task, exc):
                        self.run_plugins('ignored_exception')
                        exceptions[task] = exc
                        task.set_error()
                        log.info(f'Handled {exc!r} from {task!r}')
                        do_step = True
                        continue
                    raise exc
            action, task, progress = step_or_exception
            progress_line = ' '.join(f'{k}={v}' for k, v in progress.items())
            tag = action.name
            if task:
                tag += f': {task.label}'
            log.debug(f'{tag}, progress: {progress_line}')
            if action is Action.EXECUTE:
                do_step = not shutdown
                if do_step:
                    n_executed += 1
                    if limit:
                        assert n_executed <= limit
                        if n_executed == limit:
                            log.info('Maximum number of executed tasks reached')
                            shutdown = True
                    log.info(f'{task}: will run')
            elif action is Action.TRAVERSE:
                if task.done():
                    do_step = False
                elif not task_filter:
                    do_step = True
                else:
                    do_step = task_filter(task)
            if not do_step:
                self._skipped = True
        log.info('Finished')
        try:
            return fut.value
        except FutureError:
            if exceptions:
                msg = f'Cannot evaluate future because of errors: {exceptions}'
                log.warning(msg)
            elif self._skipped:
                log.info('Cannot evaluate future because tasks were skipped')
            else:
                tasks_not_done = self._filter_tasks(lambda t: not t.done())
                if tasks_not_done:
                    msg = f'Task dependency cycle: {tasks_not_done}'
                    raise MonaError(msg)
                else:
                    raise
        return fut

    @wraps(_eval_async)
    async def eval_async(self, *args: Any, **kwargs: Any) -> Any:
        async with self.run_context():
            return await self._eval_async(*args, **kwargs)

    def dot_graph(self, *args: Any, **kwargs: Any) -> Any:
        """Generate :class:`~graphviz.Digraph` for the task DAG."""
        from graphviz import Digraph  # type: ignore

        tasks: Union[List[Hash], FrozenSet[Hash]]
        dot = Digraph(*args, **kwargs)
        for child, parents in self._graph.deps.items():
            task_obj = self._tasks[child]
            dot.node(child, repr(Literal(task_obj)), color=STATE_COLORS[task_obj.state])
            for parent in parents:
                dot.edge(child, parent)
        for origin, tasks in self._graph.side_effects.items():
            for task in tasks:
                dot.edge(origin, task, style='dotted')
        for target, tasks in self._graph.backflow.items():
            for task in tasks:
                dot.edge(
                    task,
                    target,
                    style='tapered',
                    penwidth='7',
                    dir='back',
                    arrowtail='none',
                )
        return dot

    @classmethod
    def active(cls) -> 'Session':
        """Return a currently active session."""
        session = _active_session.get()
        if session is None:
            raise MonaError('No active session')
        return session
