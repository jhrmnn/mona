# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Any, Dict, Callable

Hook = Callable[..., Any]


class Hookable:
    def __init__(self) -> None:
        self._hooks: Dict[str, Hook] = {}

    def register_hook(self, hook_type: str) -> Callable[[Hook], Hook]:
        def decorator(hook: Hook) -> Hook:
            self._hooks[hook_type] = hook
            return hook
        return decorator

    def has_hook(self, hook_type: str) -> bool:
        return hook_type in self._hooks

    def get_hook(self, hook_type: str) -> Hook:
        return self._hooks[hook_type]
