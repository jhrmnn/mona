# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sys
from argparse import ArgumentParser

from typing import Any, Callable, TypeVar, List


_T = TypeVar('_T')
_F = TypeVar('_F', bound=Callable[..., Any])


class Arg:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs


def define_cli(cli: List[Arg]) -> Callable[[_F], _F]:
    def decorator(func: _F) -> _F:
        func.__cli__ = cli  # type: ignore
        return func
    return decorator


def exec_cli(func: Callable[..., _T], *args: Any, argv: List[str] = sys.argv) -> _T:
    parser = ArgumentParser()
    for arg in func.__cli__:  # type: ignore
        parser.add_argument(*arg.args, **arg.kwargs)
    kwargs = {k: v for k, v in vars(parser.parse_args()).items() if v}
    return func(*args, **kwargs)
