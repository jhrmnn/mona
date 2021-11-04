# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import inspect
from functools import wraps
from typing import Any, Callable, Generic, TypeVar

from .errors import MonaError
from .pyhash import hash_function
from .sessions import Session
from .tasks import Task

_T = TypeVar('_T')


class Rule(Generic[_T]):
    """Decorator that turns a function into a rule.

    A rule is a callable that generates a task instead of actually calling the
    function.

    :param func: a function
    """

    def __init__(self, func: Callable[..., _T]) -> None:
        if not inspect.isfunction(func):
            raise MonaError(f'Task function is not a function: {func}')
        self._func = func
        wraps(func)(self)

    def _func_hash(self) -> str:
        return hash_function(self._func)

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        """Create a task.

        All arguments are passed to :class:`Task`.
        """
        return Session.active().create_task(self._func, *args, **kwargs)

    @property
    def func(self) -> Callable[..., _T]:
        """Function associated with the rule."""
        return self._func
