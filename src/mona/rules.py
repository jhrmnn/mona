# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import inspect
from functools import wraps
from typing import Any, Generic, TypeVar

from .errors import MonaError
from .pyhash import hash_function
from .sessions import Session
from .tasks import Corofunc, Task

_T = TypeVar('_T')


class Rule(Generic[_T]):
    """Decorator that turns a coroutine function into a rule.

    A rule is a callable that generates a task instead of actually calling the
    coroutine.

    :param corofunc: a coroutine function
    """

    def __init__(self, corofunc: Corofunc[_T]) -> None:
        if not inspect.iscoroutinefunction(corofunc):
            raise MonaError(f'Task function is not a coroutine: {corofunc}')
        self._corofunc = corofunc
        wraps(corofunc)(self)

    def _func_hash(self) -> str:
        return hash_function(self._corofunc)

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        """Create a task.

        All arguments are passed to :class:`Task`.
        """
        return Session.active().create_task(self._corofunc, *args, **kwargs)

    @property
    def corofunc(self) -> Corofunc[_T]:
        """Coroutine function associated with the rule."""
        return self._corofunc
