# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from enum import IntEnum
from typing import Callable, Iterable, List, NoReturn, Set, TypeVar
from typing_extensions import Final

from .errors import MonaError

__all__ = ()

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_Fut = TypeVar('_Fut', bound='Future')
Callback = Callable[[_T], None]


class State(IntEnum):
    PENDING = 0
    READY = 1
    RUNNING = 2
    ERROR = 3
    HAS_RUN = 4
    AWAITING = 5
    DONE = 6


STATE_COLORS: Final = {
    State.PENDING: None,
    State.READY: 'magenta',
    State.RUNNING: 'yellow',
    State.ERROR: 'red',
    State.AWAITING: 'cyan',
    State.DONE: 'green',
}


class Future:
    def __init__(self: _Fut, parents: Iterable[_Fut]) -> None:
        self._parents = frozenset(parents)
        self._pending = {fut for fut in self._parents if not fut.done()}
        self._children: Set['Future'] = set()
        self._done_callbacks: List[Callback[_Fut]] = []
        self._ready_callbacks: List[Callback[_Fut]] = []
        self._registered = False
        self._state: State = State.PENDING if self._pending else State.READY

    def __getstate__(self) -> NoReturn:
        raise MonaError('Future objects cannot be pickled')

    @property
    def state(self) -> State:
        return self._state

    def done(self) -> bool:
        return self._state is State.DONE

    def add_child(self, fut: 'Future') -> None:
        assert not self.done()
        self._children.add(fut)

    def register(self: _Fut) -> None:
        if not self._registered:
            self._registered = True
            log.debug(f'registered: {self!r}')
            for fut in self._pending:
                fut.register()
                fut.add_child(self)

    def add_ready_callback(self: _Fut, callback: Callback[_Fut]) -> None:
        if self._state >= State.READY:
            callback(self)
        else:
            self._ready_callbacks.append(callback)

    def add_done_callback(self: _Fut, callback: Callback[_Fut]) -> None:
        assert not self.done()
        self._done_callbacks.append(callback)

    def parent_done(self: _Fut, fut: _Fut) -> None:
        assert self._state is State.PENDING
        self._pending.remove(fut)
        if not self._pending:
            self._state = State.READY
            log.debug(f'{self}: ready')
            for callback in self._ready_callbacks:
                callback(self)

    def set_done(self: _Fut) -> None:
        assert State.READY <= self._state < State.DONE
        self._state = State.DONE
        log.debug(f'{self}: done')
        for fut in self._children:
            fut.parent_done(self)
        for callback in self._done_callbacks:
            callback(self)
