# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
from typing import List

from .app import Caf, Executor, UnfinishedTask
from .Utils import get_timestamp
from .Glob import match_glob


class Dispatcher:
    def __init__(self, app: Caf, n: int, patterns: List[str] = None,
                 limit: int = None) -> None:
        self._sem = asyncio.Semaphore(n)
        self._patterns = patterns
        self._limit = limit
        app.register_hook('dispatch')(self._wrap)

    def _wrap(self, exe: Executor, label: str) -> Executor:
        async def dispatched_executor(inp: bytes) -> bytes:
            if self._patterns and not any(
                    match_glob(label, patt) for patt in self._patterns):
                raise UnfinishedTask()
            await self._sem.acquire()
            print(f'{get_timestamp()}: will execute {label}')
            out = await exe(inp)
            self._sem.release()
            print(f'{get_timestamp()}: {label} done')
            return out
        return dispatched_executor
