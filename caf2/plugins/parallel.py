# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Callable, Awaitable, Any, TypeVar, AsyncGenerator

from ..sessions import Session, SessionPlugin, running_task

_T = TypeVar('_T')


class Parallel(SessionPlugin):
    name = 'parallel'

    def __init__(self, ncores: int = None) -> None:
        self._ncores = ncores or os.cpu_count() or 1

    def post_enter(self, sess: Session) -> None:
        sess.storage['scheduler'] = self.run_coro

    def pre_eval(self, sess: Session) -> None:
        self._sem = asyncio.BoundedSemaphore(self._ncores)
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def _acquire(self, ncores: int) -> AsyncGenerator[None, None]:
        async with self._lock:
            for _ in range(ncores):
                await self._sem.acquire()
        try:
            yield
        finally:
            for _ in range(ncores):
                self._sem.release()

    async def run_coro(self,
                       corofunc: Callable[..., Awaitable[_T]],
                       *args: Any,
                       **kwargs: Any) -> _T:
        ncores = running_task().storage.get('ncores', 1)
        async with self._acquire(ncores):
            return await corofunc(*args, **kwargs)
