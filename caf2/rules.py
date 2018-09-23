# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Any, Callable, TypeVar, Generic

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
