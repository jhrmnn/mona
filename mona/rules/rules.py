# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import inspect
from functools import wraps
from typing import Any, Callable, TypeVar, Generic, Optional

from ..tasks import Task, Corofunc
from ..sessions import Session
from ..errors import MonaError
from ..hashing import hash_function

_T = TypeVar('_T')


class Rule(Generic[_T]):
    """Decorator that turns a coroutine function into a rule, which is a
    callable that generates a task instead of actually calling the coroutine.

    :param corofunc: a coroutine function
    """

    def __init__(self, corofunc: Corofunc[_T]) -> None:
        if not inspect.iscoroutinefunction(corofunc):
            raise MonaError(f'Task function is not a coroutine: {corofunc}')
        self._corofunc = corofunc
        self._label: Optional[str] = None
        wraps(corofunc)(self)

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        kwargs.setdefault('label', self._label)
        assert 'rule' not in kwargs
        kwargs['rule'] = self._corofunc.__name__
        return Session.active().create_task(self._corofunc, *args, **kwargs)

    def add_label(self, label: str) -> None:
        self._label = label

    def _func_hash(self) -> str:
        return hash_function(self._corofunc)

    @property
    def corofunc(self) -> Corofunc[_T]:
        return self._corofunc


def labelled(label: str) -> Callable[[Rule[_T]], Rule[_T]]:
    """Decorator to be used on a rule that makes the rule assign a fixed label
    to all the tasks it generates.

    :param label: a label
    """

    def decorator(rule: Rule[_T]) -> Rule[_T]:
        rule.add_label(label)
        return rule

    return decorator
