# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from enum import Enum
from typing import Iterable, Set, Callable, List, TypeVar, Union, Iterator, \
    Generic
from typing import Any  # noqa

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_Fut = TypeVar('_Fut', bound='Future')  # type: ignore
Callback = Callable[[_T], None]


class NoResult(Enum):
    _ = 0


class State(Enum):
    UNREGISTERED = -1
    PENDING = 0
    READY = 1
    RUNNING = 2
    HAS_RUN = 3
    DONE = 4


Maybe = Union[_T, NoResult]


class CafError(Exception):
    pass


class FutureNotDone(CafError):
    pass


class FutureHasNoDefault(CafError):
    pass


class Future(Generic[_T]):
    def __init__(self: _Fut, parents: Iterable['Future[Any]']) -> None:
        self._pending: Set['Future[Any]'] = set()
        for fut in parents:
            if not fut.done():
                self._pending.add(fut)
        self._children: Set['Future[Any]'] = set()
        self._result: Maybe[_T] = NoResult._
        self._registered = False
        self._done_callbacks: List[Callback[_Fut]] = []
        self._ready_callbacks: List[Callback[_Fut]] = []

    def ready(self) -> bool:
        return not self._pending

    def done(self) -> bool:
        return self._result is not NoResult._

    @property
    def state(self) -> State:
        if self.done():
            return State.DONE
        if self.ready():
            return State.READY
        if self._registered:
            return State.PENDING
        return State.UNREGISTERED

    @property
    def pending(self) -> Iterator['Future[Any]']:
        yield from self._pending

    def add_child(self, fut: 'Future[Any]') -> None:
        self._children.add(fut)

    def register(self: _Fut) -> bool:
        if not self._registered:
            self._registered = True
            for fut in self._pending:
                fut.register()
                fut.add_child(self)
            return True
        return False

    def add_ready_callback(self: _Fut, callback: Callback[_Fut]) -> None:
        if self.ready():
            callback(self)
        else:
            self._ready_callbacks.append(callback)

    def add_done_callback(self: _Fut, callback: Callback[_Fut]) -> None:
        assert not self.done()
        self._done_callbacks.append(callback)

    def default_result(self) -> Maybe[_T]:
        return NoResult._

    def result(self, check_done: bool = True) -> _T:
        if not isinstance(self._result, NoResult):  # mypy limitation
            return self._result
        if not check_done:
            result = self.default_result()
            if isinstance(result, NoResult):
                raise FutureHasNoDefault(repr(self))
            return result
        raise FutureNotDone(repr(self))

    def parent_done(self: _Fut, fut: 'Future[Any]') -> None:
        self._pending.remove(fut)
        if self.ready():
            log.debug(f'{self}: ready')
            for callback in self._ready_callbacks:
                callback(self)

    def set_result(self: _Fut, result: _T) -> None:
        assert self.ready()
        assert self._result is NoResult._
        self._result = result
        log.debug(f'{self}: done')
        for fut in self._children:
            fut.parent_done(self)
        for callback in self._done_callbacks:
            callback(self)
