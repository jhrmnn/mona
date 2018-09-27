# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Any, Callable, TypeVar, Generic

from ..tasks import Task
from ..sessions import Session

_T = TypeVar('_T')


class Rule(Generic[_T]):
    def __init__(self, func: Callable[..., _T]) -> None:
        self._func = func

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        return Session.active().create_task(self.func, *args, **kwargs)

    @property
    def func(self) -> Callable[..., _T]:
        return self._func


class HookedRule(Rule[_T]):
    def __init__(self, func: Callable[..., _T], hook: str) -> None:
        Rule.__init__(self, func)
        self._hook = hook

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        hooks = Session.active().storage.get(f'hook:{self._hook}')
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
        return HookedRule(rule.func, name)
    return decorator
