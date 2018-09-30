# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Callable, Awaitable, Any, TypeVar, AsyncGenerator, Set

from ..graph import NodeExecuted
from ..tasks import Task
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

    def wrap_execute(self, execute: TaskExecute) -> TaskExecute:
        async def _execute(task: Task[Any], reg: NodeExecuted[Task[Any]]) -> None:
            try:
                await execute(task, reg)
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    reg((e, ()))
            current_task = asyncio.current_task()
            assert current_task
            self._asyncio_tasks.remove(current_task)

        async def spawn_execute(task: Task[Any], reg: NodeExecuted[Task[Any]]
                                ) -> None:
            asyncio_task = asyncio.create_task(_execute(task, reg))
            self._asyncio_tasks.add(asyncio_task)

        return spawn_execute

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
            waited = True
        else:
            waited = False
        async with self._acquire(n):
            if waited:
                log.debug(f'All cores available for "{task}", calling')
            return await corofunc(*args, **kwargs)
