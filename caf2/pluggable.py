# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Dict, Iterable, Any, TypeVar, cast

_T = TypeVar('_T')


class Plugin:
    name: str

    def __call__(self, obj: 'Pluggable') -> None:
        obj.register_plugin(self.name, self)


class Pluggable:
    def __init__(self, plugins: Iterable[Plugin] = None) -> None:
        self._plugins: Dict[str, Plugin] = {}
        for plugin in plugins or ():
            plugin(self)

    def register_plugin(self, name: str, plugin: Plugin) -> None:
        self._plugins[name] = plugin

    def run_plugins_accum(self, func: str, start: _T, *args: Any,
                          reverse: bool = False, **kwargs: Any) -> _T:
        plugins: Iterable[Plugin] = self._plugins.values()
        if reverse:
            plugins = reversed(list(plugins))
        for plugin in plugins:
            all_args = args if start is None else (start, *args)
            start = cast(_T, getattr(plugin, func)(*all_args, **kwargs))
        return start

    def run_plugins(self, func: str, *args: Any, **kwargs: Any) -> None:
        self.run_plugins_accum(func, None, *args, **kwargs)
