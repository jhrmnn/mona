# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
from configparser import ConfigParser
from collections import OrderedDict
import os
import asyncio
from contextlib import contextmanager

from .Utils import get_timestamp, config_group
from .Remote import Remote, Local
from .Logging import error, info
from .ctx import Context
from .hooks import Hookable

from typing import Any, Dict, List, Optional, Callable, Awaitable, Iterator

Cscript = Callable[[Context], Any]
RouteFunc = Callable[[], Any]
Executor = Callable[[bytes], Awaitable[bytes]]


class RemoteNotExists(Exception):
    pass


class Caf(Hookable):
    def __init__(self) -> None:
        super().__init__()
        self.cafdir = Path(os.environ.get('CAF_DIR', '.caf'))
        self.config = ConfigParser()
        self.config.read([
            self.cafdir/'config.ini',
            Path('~/.config/caf/config.ini').expanduser()
        ])
        self.remotes = {
            name: Remote(r['host'], r['path'])
            for name, r in config_group(self.config, 'remote')
        }
        self.remotes['local'] = Local()
        self.out = Path('build')
        self.paths: List[str] = []
        self.cscripts: Dict[str, Cscript] = OrderedDict()
        self._routes: Dict[str, RouteFunc] = OrderedDict()
        self._executors: Dict[str, Executor] = {}
        self._ctx: Optional[Context] = None

    def register(self, label: str) -> Callable[[Cscript], Cscript]:
        def decorator(cscript: Cscript) -> Cscript:
            self.cscripts[label] = cscript
            return cscript
        return decorator

    def register_route(self, label: str) -> Callable[[RouteFunc], RouteFunc]:
        def decorator(route_func: RouteFunc) -> RouteFunc:
            self._routes[label] = route_func
            return route_func
        return decorator

    def register_exec(self, execid: str) -> Callable[[Executor], Executor]:
        def decorator(exe: Executor) -> Executor:
            self._executors[execid] = exe
            return exe
        return decorator

    def parse_remotes(self, remotes: str) -> List[Remote]:
        if remotes == 'all':
            return [r for r in self.remotes.values() if not isinstance(r, Local)]
        try:
            return [self.remotes[r] for r in remotes.split(',')]
        except KeyError:
            pass
        raise RemoteNotExists(remotes)

    @property
    def last_queue(self) -> str:
        try:
            return (self.cafdir/'LAST_QUEUE').read_text()
        except FileNotFoundError:
            error('No queue was ever submitted, cannot use --last')

    @last_queue.setter
    def last_queue(self, queue: str) -> None:
        (self.cafdir/'LAST_QUEUE').write_text(queue)

    def get_queue_url(self, queue: str) -> str:
        qid: Optional[str]
        if ':' in queue:
            name, qid = queue.rsplit(':', 1)
        else:
            name, qid = queue, None
        section = f'queue "{name}"'
        if not self.config.has_section(section):
            return queue
        conf = self.config[section]
        url = f'{conf["host"]}/token/{conf["token"]}'
        if qid:
            url += f'/queue/{qid}'
        return url

    @contextmanager
    def context(self, execution: bool = False) -> Iterator[None]:
        self._ctx = Context(None, app=self, conf_only=not execution)  # type: ignore
        try:
            yield
        finally:
            self._ctx = None

    @property
    def ctx(self) -> Context:
        assert self._ctx
        return self._ctx

    def get(self, route: str) -> Any:
        from .cellar import Cellar

        cellar = Cellar(self)
        ctx = Context(cellar, app=self)
        return asyncio.get_event_loop().run_until_complete(self.cscripts[route](ctx))

    def get_route(self, route: str) -> Any:
        result = asyncio.get_event_loop().run_until_complete(self._routes[route]())
        if self.has_hook('postget'):
            self.get_hook('postget')()
        return result

    def init(self) -> None:
        if not self.cafdir.is_dir():
            self.cafdir.mkdir()
            info(f'Initializing an empty repository in {self.cafdir.resolve()}.')
            if self.config.has_option('core', 'cache'):
                ts = get_timestamp()
                path = Path(self.config['core']['cache'])/f'{Path.cwd().name}_{ts}'
                path.mkdir()
                (self.cafdir/'objects').symlink_to(path)
            else:
                (self.cafdir/'objects').mkdir()
