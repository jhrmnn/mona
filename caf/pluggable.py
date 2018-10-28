# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from typing import Dict, Any, TypeVar, cast, List, Generator, Awaitable, Generic

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_P = TypeVar('_P', bound='Pluggable')


class Plugin(Generic[_P]):
    name: str

    def __call__(self, pluggable: _P) -> None:
        pluggable.register_plugin(self._name, self)

    @property
    def _name(self) -> str:
        return cast(str, getattr(self, 'name', self.__class__.__name__))


class Pluggable:
    def __init__(self: _P) -> None:
        self._plugins: Dict[str, Plugin[_P]] = {}

    def register_plugin(self: _P, name: str, plugin: Plugin[_P]) -> None:
        self._plugins[name] = plugin

    def _get_plugins(self: _P, reverse: bool = False) -> List[Plugin[_P]]:
        plugins = list(self._plugins.values())
        if reverse:
            plugins.reverse()
        return plugins

    def _run_plugins(
        self, func: str, start: Any, *args: Any, reverse: bool = False, **kwargs: Any
    ) -> Generator[Any, Any, None]:
        for plugin in self._get_plugins():
            all_args = args if start is None else (start, *args)
            try:
                start = yield getattr(plugin, func)(*all_args, **kwargs)
            except Exception:
                log.error(f'Error in plugin {plugin._name!r}')
                raise

    async def run_plugins_async(
        self, func: str, *args: Any, start: _T, reverse: bool = False, **kwargs: Any
    ) -> _T:
        gen = self._run_plugins(func, start, *args, reverse=reverse, **kwargs)
        try:
            start = await cast(Awaitable[_T], next(gen))
            while True:
                start = await cast(Awaitable[_T], gen.send(start))
        except StopIteration:
            return start

    def run_plugins(
        self, func: str, *args: Any, start: _T, reverse: bool = False, **kwargs: Any
    ) -> _T:
        gen = self._run_plugins(func, start, *args, reverse=reverse, **kwargs)
        try:
            start = cast(_T, next(gen))
            while True:
                start = cast(_T, gen.send(start))
        except StopIteration:
            return start
