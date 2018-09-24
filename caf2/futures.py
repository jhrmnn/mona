# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from enum import IntEnum
from typing import Iterable, Set, Callable, List, TypeVar, Iterator, Generic, \
    NoReturn, FrozenSet
from typing import Any  # noqa

from .utils import CafError, Maybe, Empty

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_Fut = TypeVar('_Fut', bound='Future')  # type: ignore
Callback = Callable[[_T], None]


class State(IntEnum):
    PENDING = 0
    READY = 1
    RUNNING = 2
    HAS_RUN = 3
    DONE = 4


class FutureNotDone(CafError):
    pass


class FutureHasNoDefault(CafError):
    pass


class Future(Generic[_T]):
    def __init__(self: _Fut, parents: Iterable['Future[Any]']) -> None:
        self._parents = frozenset(parents)
        self._pending = set(fut for fut in self._parents if not fut.done())
        self._children: Set['Future[Any]'] = set()
        self._result: Maybe[_T] = Empty._
        self._done_callbacks: List[Callback[_Fut]] = []
        self._ready_callbacks: List[Callback[_Fut]] = []
        self._registered = False
        self._state: State = State.PENDING if self._pending else State.READY

    def __getstate__(self) -> NoReturn:
        raise CafError('Future objects cannot be pickled')

    @property
    def state(self) -> State:
        return self._state

    def done(self) -> bool:
        return self._state is State.DONE

    @property
    def parents(self) -> FrozenSet['Future[Any]']:
        return self._parents

    def add_child(self, fut: 'Future[Any]') -> None:
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

    # can be overriden by derived classes
    def default_result(self) -> Maybe[_T]:
        assert not self.done()
        return Empty._

    def result(self, check_done: bool = True) -> _T:
        if not isinstance(self._result, Empty):
            return self._result
        if not check_done:
            result = self.default_result()
            if isinstance(result, Empty):
                raise FutureHasNoDefault(repr(self))
            return result
        raise FutureNotDone(repr(self))

    def parent_done(self: _Fut, fut: 'Future[Any]') -> None:
        assert self._state is State.PENDING
        self._pending.remove(fut)
        if not self._pending:
            self._state = State.READY
            log.debug(f'{self}: ready')
            for callback in self._ready_callbacks:
                callback(self)

    def set_result(self: _Fut, result: _T) -> None:
        assert State.READY <= self._state < State.DONE
        self._result = result
        self._state = State.DONE
        log.debug(f'{self}: done')
        for fut in self._children:
            fut.parent_done(self)
        for callback in self._done_callbacks:
            callback(self)
