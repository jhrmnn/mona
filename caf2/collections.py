# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import TypeVar, Generic, Deque, Set

_T = TypeVar('_T')


class HashedDeque(Generic[_T]):
    def __init__(self) -> None:
        self._deque = Deque[_T]()
        self._deque_set: Set[_T] = set()

    def __bool__(self) -> bool:
        return bool(self._deque)

    def __contains__(self, item: _T) -> bool:
        return item in self._deque_set

    def append(self, item: _T) -> None:
        self._deque.append(item)
        self._deque_set.add(item)

    def popleft(self) -> _T:
        item = self._deque.popleft()
        self._deque_set.remove(item)
        return item
