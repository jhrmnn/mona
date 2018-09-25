# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import stat
from enum import Enum
from typing import Any, Callable, TypeVar, Union, List, Tuple, Iterable

_T = TypeVar('_T')
Maybe = Union[_T, 'Empty']
Pathable = Union[str, bytes, 'os.PathLike[Any]']


class CafError(Exception):
    pass


# Ideally Empty.EMPTY could be used directly, but mypy doesn't understand that
# yet, so isisntance() it is.
class Empty(Enum):
    """Absence of a value."""
    _ = 0


def get_fullname(obj: Callable[[Any], Any]) -> str:
    return f'{obj.__module__}:{obj.__qualname__}'


def shorten_text(s: Union[str, bytes], n: int) -> str:
    if len(s) > n:
        s = s[:n-3]
        shortened = True
    else:
        shortened = False
    text = s.decode() if isinstance(s, bytes) else s
    return f'{text.rstrip()}...' if shortened else text


class Literal(str):
    def __repr__(self) -> str:
        return self


def make_executable(path: Pathable) -> None:
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def make_nonwritable(path: Pathable) -> None:
    os.chmod(
        path,
        stat.S_IMODE(os.lstat(path).st_mode) &
        ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    )


def split(iterable: Iterable[_T], first: Callable[[_T], bool]
          ) -> Tuple[List[_T], List[_T]]:
    left: List[_T] = []
    right: List[_T] = []
    for item in iterable:
        (left if first(item) else right).append(item)
    return left, right
