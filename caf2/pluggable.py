# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Dict, Iterable, Any, TypeVar, cast, List, \
    Generator, Awaitable

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

    def _get_plugins(self, reverse: bool = False) -> List[Plugin]:
        plugins = list(self._plugins.values())
        if reverse:
            plugins.reverse()
        return plugins

    def _run_plugins(self, func: str, start: Any, *args: Any,
                     reverse: bool = False, **kwargs: Any
                     ) -> Generator[Any, Any, None]:
        for plugin in self._get_plugins():
            all_args = args if start is None else (start, *args)
            start = yield getattr(plugin, func)(*all_args, **kwargs)

    async def run_plugins_async(self, func: str, *args: Any, start: _T,
                                reverse: bool = False, **kwargs: Any) -> _T:
        gen = self._run_plugins(func, start, *args, reverse=reverse, **kwargs)
        try:
            start = await cast(Awaitable[_T], next(gen))
            while True:
                start = await cast(Awaitable[_T], gen.send(start))
        except StopIteration:
            return start

    def run_plugins(self, func: str, *args: Any, start: _T,
                    reverse: bool = False, **kwargs: Any) -> _T:
        gen = self._run_plugins(func, start, *args, reverse=reverse, **kwargs)
        try:
            start = cast(_T, next(gen))
            while True:
                start = cast(_T, gen.send(start))
        except StopIteration:
            return start
