# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
from collections import OrderedDict
import os
import asyncio
from contextlib import contextmanager

from .hooks import Hookable

from typing import Any, Dict, List, Optional, Callable, Awaitable, Iterator

RouteFunc = Callable[[], Any]
Executor = Callable[[bytes], Awaitable[bytes]]

CAFDIR = Path(os.environ.get('CAF_DIR', '.caf')).resolve()


class Context:
    def __init__(self, noexec: bool = True, readonly: bool = True) -> None:
        self.noexec = noexec
        self.readonly = readonly
        self.g: Dict[str, Any] = {}


class Caf(Hookable):
    def __init__(self, cafdir: Path = None) -> None:
        super().__init__()
        self.cafdir = cafdir.resolve() if cafdir else CAFDIR
        self.paths: List[str] = []
        self._routes: Dict[str, RouteFunc] = OrderedDict()
        self._executors: Dict[str, Executor] = {}
        self._ctx: Optional[Context] = None

    async def task(self, execid: str, inp: bytes, label: str = None) -> bytes:
        exe = self._executors[execid]
        if self.has_hook('dispatch'):
            assert label
            exe = self.get_hook('dispatch')(exe, label)
        if self.has_hook('cache'):
            assert label
            return await self.get_hook('cache')(exe, execid, inp, label)  # type: ignore
        return await exe(inp)

    def route(self, label: str) -> Callable[[RouteFunc], RouteFunc]:
        def decorator(route_func: RouteFunc) -> RouteFunc:
            self._routes[label] = route_func
            return route_func
        return decorator

    def register_exec(self, execid: str, exe: Executor) -> None:
        self._executors[execid] = exe

    @contextmanager
    def context(self, execution: bool = False, readonly: bool = True) -> Iterator[None]:
        self._ctx = Context(noexec=not execution, readonly=readonly)
        try:
            yield
        finally:
            self._ctx = None

    @property
    def ctx(self) -> Context:
        assert self._ctx
        return self._ctx

    def get(self, *routes: str) -> Any:
        result = asyncio.get_event_loop().run_until_complete(
            asyncio.gather(*(self._routes[route]() for route in routes))
        )
        if self.has_hook('postget'):
            self.get_hook('postget')()
        return result[0] if len(routes) == 1 else result
