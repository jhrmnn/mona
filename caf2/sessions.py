# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import warnings
from contextlib import contextmanager
from typing import Set, Any, Dict, Callable, Optional, List, Deque, \
    TypeVar, Iterator

from .futures import Future, CafError, FutureNotDone
from .tasks import Task, Hash, HashedFuture, TaskComposite, ensure_future, State
from .collections import HashedDeque

log = logging.getLogger(__name__)

_T = TypeVar('_T')


def extract_tasks(fut: Future[Any]) -> Set[Task[Any]]:
    tasks: Set[Task[Any]] = set()
    visited: Set[Future[Any]] = set()
    queue = Deque[Future[Any]]()
    queue.append(fut)
    while queue:
        fut = queue.popleft()
        visited.add(fut)
        if isinstance(fut, Task):
            tasks.add(fut)
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


class Session:
    _active: Optional['Session'] = None

    def __init__(self) -> None:
        self._tasks: Dict[Hash, Task[Any]] = {}
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
                warnings.warn(f'tasks were never run: {tasks_not_run}', RuntimeWarning)
        self._tasks.clear()

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
        for arg in task.args:
            if isinstance(arg, Task):
                if arg not in self:
                    raise ArgNotInSession(repr(arg))
            else:
                for arg_task in extract_tasks(arg):
                    if arg_task not in self:
                        raise ArgNotInSession(f'{arg!r} -> {arg_task!r}')
        task.register()
        self._tasks[task.hashid] = task
        return task

    def run_task(self, task: Task[_T], check_ready: bool = True
                 ) -> Optional[_T]:
        assert not task.done()
        if check_ready:
            assert task.state > State.PENDING
        args = [arg.result(check_done=check_ready) for arg in task.args]
        with self.record(task.children):
            result = task.func(*args)
        if task.children:
            log.debug(f'{task}: created children: {list(map(str, task.children))}')
        if task.state is State.PENDING:
            return result
        fut: Optional[HashedFuture[_T]] = None
        if isinstance(result, HashedFuture):
            fut = result
        else:
            comp = TaskComposite.from_object(result)
            if comp.has_futures():
                fut = comp
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
        fut = ensure_future(obj)
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
        try:
            return fut.result()
        except FutureNotDone as e:
            raise DependencyCycle(
                [task for task in self._tasks.values() if not task.done()]
            ) from e

    @classmethod
    def active(cls) -> 'Session':
        if cls._active is None:
            raise NoActiveSession()
        return cls._active
