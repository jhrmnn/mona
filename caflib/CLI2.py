# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Any, Callable, TypeVar, List


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
