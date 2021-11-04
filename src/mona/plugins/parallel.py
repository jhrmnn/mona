# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import os
import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional, Set, TypeVar

from ..sessions import (
    Session,
    SessionPlugin,
    TaskExecuted,
    TaskExecutor,
    _active_session,
)
from ..tasks import Task

log = logging.getLogger(__name__)

_T = TypeVar('_T')


class Parallel(SessionPlugin):
    """Plugin that enables running tasks in parallel."""

    name = 'parallel'

    def __init__(self, ncores: int = None) -> None:
        self._ncores = ncores or os.cpu_count() or 1
        self._available = self._ncores
        self._threads: Set[threading.Thread] = set()
        self._pending: Optional[int] = None
        self._registered_exceptions = 0

    def post_enter(self, sess: Session) -> None:  # noqa: D102
        sess.storage['scheduler'] = self._run_func

    def pre_run(self) -> None:  # noqa: D102
        self._sem = threading.BoundedSemaphore(self._ncores)
        self._lock = threading.Lock()

    def post_run(self) -> None:  # noqa: D102
        if not self._threads:
            return
        log.info(f'Waiting for {len(self._threads)} running threads...')
        for thread in self._threads:
            thread.join()
        assert not self._threads
        log.info('All threads ended')

    def _release(self, ncores: int) -> None:
        if self._pending is None:
            for _ in range(ncores):
                self._sem.release()
            self._available += ncores
        else:
            self._pending += ncores

    def _stop(self) -> None:
        assert self._pending is None
        self._pending = 0
        log.info('Stopping scheduler')

    def ignored_exception(self) -> None:  # noqa: D102
        if self._registered_exceptions == 0:
            return
        self._registered_exceptions -= 1
        if self._registered_exceptions > 0:
            return
        assert self._pending is not None
        log.info(f'Resuming scheduler with {self._pending} cores')
        pending = self._pending
        self._pending = None
        self._release(pending)

    def wrap_execute(self, execute: TaskExecutor) -> TaskExecutor:  # noqa: D102
        def _execute(
            task: Task[object], done: TaskExecuted, session: Optional[Session]
        ) -> None:
            assert not _active_session.get()
            _active_session.set(session)
            try:
                assert execute(task, done)
            except Exception as e:
                done((task, e, ()))
            thread = threading.current_thread()
            self._threads.remove(thread)

        def spawn_execute(*args: Any) -> bool:
            thread = threading.Thread(
                target=_execute, args=args, kwargs={'session': _active_session.get()}
            )
            self._threads.add(thread)
            thread.start()
            return True

        return spawn_execute

    @contextmanager
    def _acquire(self, ncores: int) -> Iterator[None]:
        with self._lock:
            for _ in range(ncores):
                self._sem.acquire()
                self._available -= 1
        try:
            yield
        except Exception:
            if self._registered_exceptions == 0:
                self._stop()
            self._registered_exceptions += 1
            raise
        finally:
            self._release(ncores)

    def _run_func(self, func: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        task = Session.active().running_task
        n: Optional[int] = kwargs.get('ncores')
        if n is not None:
            if n == -1:
                n = self._ncores
            kwargs['ncores'] = n
        else:
            n = 1
        if n > self._available:
            log.debug(
                f'Waiting for {n-self._available}/{n} unavailable cores for {task}'
            )
            waited = True
        else:
            waited = False
        with self._acquire(n):
            if waited:
                log.debug(f'All {n} cores available for "{task}", resuming')
            return func(*args, **kwargs)
