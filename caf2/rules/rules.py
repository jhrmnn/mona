# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import inspect
from typing import Any, Callable, TypeVar, Generic, Tuple, Optional

from ..tasks import Task, Corofunc
from ..sessions import Session
from ..errors import CafError
from ..hashing import hash_function

_T = TypeVar('_T')
InputHook = Callable[[Tuple[Any, ...]], Tuple[Any, ...]]
OutputHook = Callable[[_T], _T]
Hooks = Optional[Tuple[Optional[InputHook], Optional[OutputHook[_T]]]]


class Rule(Generic[_T]):
    def __init__(self, corofunc: Corofunc[_T]) -> None:
        if not inspect.iscoroutinefunction(corofunc):
            raise CafError(f'Task function is not a coroutine: {corofunc}')
        self._corofunc = corofunc
        self._label: Optional[str] = None

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        kwargs.setdefault('label', self._label)
        return Session.active().create_task(self._corofunc, *args, **kwargs)

    def add_label(self, label: str) -> None:
        self._label = label

    def _func_hash(self) -> str:
        return hash_function(self._corofunc)

    @property
    def corofunc(self) -> Corofunc[_T]:
        return self._corofunc


class HookedRule(Rule[_T]):
    def __init__(self, corofunc: Corofunc[_T], hook: str) -> None:
        Rule.__init__(self, corofunc)
        self._hook = hook

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        hooks: Hooks[_T] = Session.active().storage.get(f'hook:{self._hook}')
        if hooks:
            pre_hook, post_hook = hooks
            if pre_hook:
                args = pre_hook(args)
        task = Rule.__call__(self, *args, **kwargs)
        if hooks and post_hook:
            task.add_hook(post_hook)
        return task


def with_hook(name: str) -> Callable[[Rule[_T]], HookedRule[_T]]:
    def decorator(rule: Rule[_T]) -> HookedRule[_T]:
        return HookedRule(rule.corofunc, name)
    return decorator


def labelled(label: str) -> Callable[[Rule[_T]], Rule[_T]]:
    def decorator(rule: Rule[_T]) -> Rule[_T]:
        rule.add_label(label)
        return rule
    return decorator
