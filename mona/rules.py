# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import inspect
from functools import wraps
from typing import Any, TypeVar, Generic, List, Callable

from .tasks import Task, Corofunc
from .sessions import Session
from .errors import MonaError
from .hashing import Hashed
from .pyhash import hash_function

_T = TypeVar('_T')
ArgFactory = Callable[[], Hashed[object]]


class Rule(Generic[_T]):
    """Decorator that turns a coroutine function into a rule, which is a
    callable that generates a task instead of actually calling the coroutine.

    :param corofunc: a coroutine function
    """

    def __init__(self, corofunc: Corofunc[_T]) -> None:
        if not inspect.iscoroutinefunction(corofunc):
            raise MonaError(f'Task function is not a coroutine: {corofunc}')
        self._corofunc = corofunc
        self._extra_arg_factories: List[ArgFactory] = []
        wraps(corofunc)(self)

    def _ensure_extra_args(self) -> None:
        if not hasattr(self, '_hash'):
            self._extra_args = [factory() for factory in self._extra_arg_factories]
            hashes = [
                hash_function(self._corofunc),
                *(obj.hashid for obj in self._extra_args),
            ]
            self._hash = ','.join(hashes)

    def _func_hash(self) -> str:
        self._ensure_extra_args()
        return self._hash

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        self._ensure_extra_args()
        assert 'rule' not in kwargs
        kwargs['rule'] = self._corofunc.__name__
        return Session.active().create_task(
            self._corofunc, *args, *self._extra_args, **kwargs
        )

    def add_extra_arg(self, factory: ArgFactory) -> None:
        assert not hasattr(self, '_extra_args')
        self._extra_arg_factories.append(factory)

    @property
    def corofunc(self) -> Corofunc[_T]:
        return self._corofunc
