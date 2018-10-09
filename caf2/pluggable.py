# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from typing import Dict, Iterable, Any, TypeVar, cast, List, \
    Generator, Awaitable, Generic

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_Plg = TypeVar('_Plg', bound='Plugin')


class Plugin:
    name: str

    def __call__(self: _Plg, obj: 'Pluggable[_Plg]') -> None:
        obj.register_plugin(self._name, self)

    @property
    def _name(self) -> str:
        return cast(str, getattr(self, 'name', self.__class__.__name__))


class Pluggable(Generic[_Plg]):
    def __init__(self, plugins: Iterable[_Plg] = None) -> None:
        self._plugins: Dict[str, _Plg] = {}
        for plugin in plugins or ():
            plugin(self)  # type: ignore

    def register_plugin(self, name: str, plugin: _Plg) -> None:
        self._plugins[name] = plugin

    def _get_plugins(self, reverse: bool = False) -> List[_Plg]:
        plugins = list(self._plugins.values())
        if reverse:
            plugins.reverse()
        return plugins

    def _run_plugins(self, func: str, start: Any, *args: Any,
                     reverse: bool = False, **kwargs: Any
                     ) -> Generator[Any, Any, None]:
        for plugin in self._get_plugins():
            all_args = args if start is None else (start, *args)
            try:
                start = yield getattr(plugin, func)(*all_args, **kwargs)
            except Exception:
                log.error(f'Error in plugin {plugin._name!r}')
                raise

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
