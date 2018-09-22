# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Any, Callable, TypeVar, Union, overload, Generic

from .tasks import Task
from .sessions import Session

_T = TypeVar('_T')


class Rule(Generic[_T]):
    def __init__(self, func: Callable[..., _T], **kwargs: Any) -> None:
        self._func = func
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'<Rule func={self._func!r} kwargs={self._kwargs!r}>'

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        return Session.active().create_task(
            self._func, *args, **self._kwargs, **kwargs
        )


@overload
def rule(func: Callable[..., _T]) -> Rule[_T]: ...
@overload  # noqa
def rule(*, label: str = None, default: Any = None
         ) -> Callable[[Callable[..., _T]], Rule[_T]]: ...


def rule(*args: Callable[..., _T], **kwargs: Any
         ) -> Union[Rule[_T], Callable[[Callable[..., _T]], Rule[_T]]]:
    if args:
        assert not kwargs
        func, = args
        return Rule(func)

    def decorator(func: Callable[..., _T]) -> Rule[_T]:
        return Rule(func, **kwargs)
    return decorator
