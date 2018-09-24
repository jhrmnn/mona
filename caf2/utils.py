# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from enum import Enum
from typing import Any, Callable, TypeVar, Union


_T = TypeVar('_T')
Maybe = Union[_T, 'Empty']


class Empty(Enum):
    _ = 0


class CafError(Exception):
    pass


def get_fullname(obj: Callable[[Any], Any]) -> str:
    return f'{obj.__module__}:{obj.__qualname__}'


def shorten_text(s: str, n: int) -> str:
    if len(s) < n:
        return s
    return f'{s[:n-3]}...'


class Literal(str):
    def __repr__(self) -> str:
        return self
