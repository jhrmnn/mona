# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
from typing import List
import sqlite3
import subprocess
import shutil

from .app import Caf, Executor, UnfinishedTask
from .cellar import State
from .scheduler import Scheduler
from .Utils import get_timestamp, get_hash
from .Glob import match_glob
from .Logging import error, debug


class DispatcherStopped(Exception):
    pass


class Dispatcher:
    def __init__(self, app: Caf, scheduler: Scheduler, n: int,
                 patterns: List[str] = None, limit: int = None) -> None:
        self._sem = asyncio.Semaphore(n)
        self._patterns = patterns
        self._limit = limit
        self._nexecuted = 0
        app.register_hook('dispatch')(self._wrap)
        self._scheduler = scheduler
        self._scheduler._db.isolation_level = None

    def _wrap(self, exe: Executor, label: str) -> Executor:
        async def dispatched_executor(inp: bytes) -> bytes:
            if self._limit is not None and self._nexecuted >= self._limit:
                msg = f'{get_timestamp()}: {self._nexecuted} tasks ran, quitting'
                raise DispatcherStopped(msg)
            if self._patterns and not any(
                    match_glob(label, patt) for patt in self._patterns):
                raise UnfinishedTask()
            await self._sem.acquire()
            hashid = get_hash(inp)
            self._scheduler._labels[hashid] = label
            try:
                state, = self._scheduler.execute(
                    'select state as "[state]" from queue where active = 1 and taskhash = ?',
                    (hashid,)
                ).fetchone()
            except sqlite3.OperationalError:
                error('There is no queue.')
            if not self._scheduler.is_state_ok(state, hashid, label):
                debug(f'{label} does not have conforming state, skipping')
                self._sem.release()
                raise UnfinishedTask()
            with self._scheduler.db_lock():
                state, = self._scheduler.execute(
                    'select state as "[state]" from queue where active = 1 and taskhash = ?',
                    (hashid,)
                ).fetchone()
                if state != State.CLEAN:
                    print(f'{label} already locked!')
                    raise UnfinishedTask()
                self._scheduler.execute(
                    'update queue set state = ?, changed = ? where taskhash = ?',
                    (State.RUNNING, get_timestamp(), hashid)
                )
            print(f'{get_timestamp()}: will execute {label}')
            try:
                out = await exe(inp)
            except asyncio.CancelledError:
                self._scheduler.task_interrupt(hashid)
                print(f'{get_timestamp()}: {label} was interrupted')
                raise
            except subprocess.CalledProcessError as e:
                print(e)
                self._scheduler.task_error(hashid)
                print(f'{get_timestamp()}: {label} finished with error')
                self._sem.release()
                raise UnfinishedTask()
            else:
                self._nexecuted += 1
                shutil.rmtree(self._scheduler._tmpdirs.pop(hashid))
                self._scheduler.task_done(hashid)
                print(f'{get_timestamp()}: {label} finished successfully')
            self._sem.release()
            return out
        return dispatched_executor
