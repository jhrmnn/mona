# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import asyncio
import logging
from functools import partial
from contextlib import asynccontextmanager
from typing import Callable, Awaitable, Any, TypeVar, AsyncGenerator, Set

from ..sessions import Session, SessionPlugin, TaskExecute

log = logging.getLogger(__name__)

_T = TypeVar('_T')


class Parallel(SessionPlugin):
    name = 'parallel'

    def __init__(self, ncores: int = None) -> None:
        self._ncores = ncores or os.cpu_count() or 1
        self._available = self._ncores
        self._asyncio_tasks: Set[asyncio.Task[Any]] = set()

    def post_enter(self, sess: Session) -> None:
        sess.storage['scheduler'] = self.run_coro

    async def pre_run(self) -> None:
        self._sem = asyncio.BoundedSemaphore(self._ncores)
        self._lock = asyncio.Lock()

    async def post_run(self) -> None:
        if not self._asyncio_tasks:
            return
        log.info(f'Cancelling {len(self._asyncio_tasks)} running tasks...')
        for task in self._asyncio_tasks:
            task.cancel()
        await asyncio.gather(*self._asyncio_tasks)
        assert not self._asyncio_tasks
        log.info('All tasks cancelled')

    async def _run_execute(self, execute: TaskExecute, *args: Any) -> None:
        exc_result = await execute(*args)
        if exc_result:
            exc, reg = exc_result
            if not isinstance(exc, asyncio.CancelledError):
                reg((exc, None))
        current_task = asyncio.current_task()
        assert current_task
        self._asyncio_tasks.remove(current_task)

    async def _spawn_execute(self, execute: TaskExecute, *args: Any) -> None:
        asyncio_task = asyncio.create_task(self._run_execute(execute, *args))
        self._asyncio_tasks.add(asyncio_task)

    def wrap_execute(self, execute: TaskExecute) -> TaskExecute:
        return partial(self._spawn_execute, execute)

    @asynccontextmanager
    async def _acquire(self, ncores: int) -> AsyncGenerator[None, None]:
        async with self._lock:
            for _ in range(ncores):
                await self._sem.acquire()
                self._available -= 1
        try:
            yield
        finally:
            for _ in range(ncores):
                self._sem.release()
            self._available += ncores

    async def run_coro(self,
                       corofunc: Callable[..., Awaitable[_T]],
                       *args: Any,
                       **kwargs: Any) -> _T:
        task = Session.active().running_task
        n = task.storage.get('ncores', 1)
        if n > self._available:
            log.debug(f'{self._available}/{n} cores available for "{task}"')
            waiting = True
        else:
            waiting = False
        async with self._acquire(n):
            if waiting:
                log.debug(f'All cores available for "{task}", calling')
            return await corofunc(*args, **kwargs)
