# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import stat
import importlib
from enum import Enum
from datetime import datetime
from typing import Any, Callable, TypeVar, Union, List, Tuple, Iterable, Dict, Type

_T = TypeVar('_T')
_V = TypeVar('_V')
Maybe = Union[_T, 'Empty']
Pathable = Union[str, 'os.PathLike[str]']
TypeSwaps = Dict[Type[Any], Callable[[Any], Any]]


# Ideally Empty.EMPTY could be used directly, but mypy doesn't understand that
# yet, so isisntance() it is.
class Empty(Enum):
    """Absence of a value."""

    _ = 0


def get_fullname(obj: Union[Callable[..., Any], Type[Any]]) -> str:
    return f'{obj.__module__}:{obj.__qualname__}'


def import_fullname(fullname: str) -> Any:
    module_name, qualname = fullname.split(':')
    module = importlib.import_module(module_name)
    return getattr(module, qualname)


def shorten_text(s: Union[str, bytes], n: int) -> str:
    if len(s) > n:
        s = s[: n - 3]
        shortened = True
    else:
        shortened = False
    text = s.decode() if isinstance(s, bytes) else s
    return f'{text.rstrip()}...' if shortened else text


class Literal(str):
    def __repr__(self) -> str:
        return str.__repr__(self)[1:-1]


def swap_type(o: Any, swaps: TypeSwaps) -> Any:
    if o.__class__ in swaps:
        return swaps[o.__class__](o)
    return o


# TODO ignore existing permissions
def make_executable(path: Pathable) -> None:
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def make_nonwritable(path: Pathable) -> None:
    os.chmod(
        path,
        stat.S_IMODE(os.lstat(path).st_mode)
        & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH),
    )


def get_timestamp() -> str:
    return datetime.now().isoformat(timespec='seconds')


def call_if(cond: bool, func: Callable[..., None], *args: Any, **kwargs: Any) -> None:
    if cond:
        func(*args, **kwargs)


def split(
    iterable: Iterable[_T], first: Callable[[_T], bool]
) -> Tuple[List[_T], List[_T]]:
    left: List[_T] = []
    right: List[_T] = []
    for item in iterable:
        (left if first(item) else right).append(item)
    return left, right


def groupby(iterable: Iterable[_T], key: Callable[[_T], _V]) -> Dict[_V, List[_T]]:
    groups: Dict[_V, List[_T]] = {}
    for x in iterable:
        groups.setdefault(key(x), []).append(x)
    return groups
