# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from typing import Any, Dict, Generator, Generic, List, Optional, TypeVar

__all__ = ()

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_P = TypeVar('_P', bound='Pluggable')


class Plugin(Generic[_P]):
    name: Optional[str] = None

    def __call__(self, pluggable: _P) -> None:
        pluggable.register_plugin(self._name, self)

    @property
    def _name(self) -> str:
        return self.name or self.__class__.__name__


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
        self, func: str, args: List[Any], wrap_first: bool, reverse: bool
    ) -> Generator[Any, Any, None]:
        for plugin in self._get_plugins(reverse):
            try:
                result = yield getattr(plugin, func)(*args)
            except Exception:
                log.error(f'Error in plugin {plugin._name!r}')
                raise
            if wrap_first:
                args[0] = result

    async def run_plugins_async(
        self, func: str, *args: Any, wrap_first: bool = False, reverse: bool = False
    ) -> Any:
        arg_list = list(args)
        gen = self._run_plugins(func, arg_list, wrap_first, reverse)
        result: Any = None
        try:
            while True:
                result = await gen.send(result)
        except StopIteration:
            if wrap_first:
                return arg_list[0]

    def run_plugins(
        self, func: str, *args: Any, wrap_first: bool = False, reverse: bool = False
    ) -> Any:
        arg_list = list(args)
        gen = self._run_plugins(func, arg_list, wrap_first, reverse)
        result: Any = None
        try:
            while True:
                result = gen.send(result)
        except StopIteration:
            if wrap_first:
                return arg_list[0]
