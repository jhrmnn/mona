# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    NamedTuple,
    Tuple,
    TypeVar,
    cast,
)

import toml

from .files import File, HashedFile
from .plugins import Cache, FileManager, Parallel, TmpdirManager
from .remotes import Remote
from .rules import Rule
from .sessions import Session
from .tasks import Task
from .utils import Pathable, get_timestamp

__all__ = ()

log = logging.getLogger(__name__)

_R = TypeVar('_R', bound=Rule[object])
ArgFactory = Callable[[str], object]


class Entry(NamedTuple):
    rule: Rule[object]
    factories: Tuple[ArgFactory, ...]
    stdout: bool


class Mona:
    MONADIR = '.mona'
    TMPDIR = 'tmpdir'
    FILES = 'files'
    CACHE = 'cache.db'
    LAST_ENTRY = 'LAST_ENTRY'

    def __init__(self, monadir: Pathable = None) -> None:
        monadir = monadir or os.environ.get('MONA_DIR') or Mona.MONADIR
        self._monadir = Path(monadir).resolve()
        self._configfile = self._monadir / 'config.toml'
        self._config: Dict[str, Any] = {}
        self._entries: Dict[str, Entry] = {}
        for path in [
            Path('~/.config/mona/config.toml').expanduser(),
            Path('mona.toml'),
            self._configfile,
        ]:
            if path.exists():
                with path.open() as f:
                    self._config.update(toml.load(f))

    def entry(
        self, name: str, *factories: ArgFactory, stdout: bool = False
    ) -> Callable[[_R], _R]:
        def decorator(rule: _R) -> _R:
            self._entries[name] = Entry(rule, factories, stdout)
            return rule

        return decorator

    def get_entry(self, name: str) -> Entry:
        return self._entries[name]

    def call_entry(self, name: str, *arg_strings: str) -> Task[object]:
        rule, factories, _ = self._entries[name]
        args = [factory(arg_str) for factory, arg_str in zip(factories, arg_strings)]
        return rule(*args)

    def create_session(self, warn: bool = False, **kwargs: Any) -> Session:
        sess = Session(warn=warn)
        self(sess, **kwargs)
        return sess

    @property
    def last_entry(self) -> List[str]:
        return cast(
            List[str], json.loads((self._monadir / Mona.LAST_ENTRY).read_text())
        )

    @last_entry.setter
    def last_entry(self, entry: List[str]) -> None:
        (self._monadir / Mona.LAST_ENTRY).write_text(json.dumps(entry))

    def call_last_entry(self) -> Task[object]:
        return self.call_entry(*self.last_entry)

    def __call__(
        self,
        sess: Session,
        ncores: int = None,
        write: str = 'eager',
        full_restore: bool = False,
    ) -> None:
        self._plugins = {
            'parallel': Parallel(ncores),
            'tmpdir': TmpdirManager(self._monadir / Mona.TMPDIR),
            'files': FileManager(self._monadir / Mona.FILES),
            'cache': Cache.from_path(
                self._monadir / Mona.CACHE, write=write, full_restore=full_restore
            ),
        }
        for plugin in self._plugins.values():
            plugin(sess)

    def ensure_initialized(self) -> None:
        if self._monadir.is_dir():
            log.info(f'Already initialized in {self._monadir}.')
            return
        log.info(f'Initializing an empty repository in {self._monadir}.')
        self._monadir.mkdir()
        try:
            cache_home = Path(self._config['cache'])
        except KeyError:
            for dirname in [Mona.TMPDIR, Mona.FILES]:
                (self._monadir / dirname).mkdir()
        else:
            ts = get_timestamp()
            cachedir = cache_home / f'{Path.cwd().name}_{ts}'
            cachedir.mkdir()
            for dirname in [Mona.TMPDIR, Mona.FILES]:
                (cachedir / dirname).mkdir()
                (self._monadir / dirname).symlink_to(cachedir / dirname)

    @contextmanager
    def update_config(self) -> Iterator[MutableMapping[str, Any]]:
        if self._configfile.exists():
            with self._configfile.open() as f:
                config = toml.load(f)
        else:
            config = {}
        yield config
        self._config.update(config)
        if config:
            with self._configfile.open('w') as f:
                toml.dump(config, f)

    def parse_remotes(self, remote_str: str) -> Iterable[Remote]:
        if remote_str == 'all':
            remotes = [r for r in self._config['remotes'].values()]
        else:
            remotes = [self._config['remotes'][name] for name in remote_str.split(',')]
        for remote in remotes:
            yield Remote(remote['host'], remote['path'])

    def add_source(self, path: Pathable) -> Callable[[_R], _R]:
        """Create a rule decorator to add a source to the task arguments.

        The source is passed as :class:`File`. The file argument is appended to the
        directly passed arguments.
        """

        def decorator(rule: _R) -> _R:
            rule.add_extra_arg(lambda: HashedFile(File.from_path(path)))
            return rule

        return decorator
