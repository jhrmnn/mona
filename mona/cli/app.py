# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import logging
from pathlib import Path
from typing import Dict, Any

import toml

from ..sessions import Session
from ..plugins import Parallel, TmpdirManager, FileManager, Cache
from ..utils import get_timestamp, Pathable

log = logging.getLogger(__name__)


class App:
    MONADIR = '.mona'
    TMPDIR = 'tmpdir'
    FILES = 'files'
    CACHE = 'cache.db'

    def __init__(self, monadir: Pathable = None) -> None:
        monadir = monadir or os.environ.get('MONA_DIR') or App.MONADIR
        self._monadir = Path(monadir).resolve()
        self._config: Dict[str, Any] = {}
        for path in [
            Path('~/.config/mona/config.toml').expanduser(),
            Path('mona.toml'),
            self._monadir / 'config.toml',
        ]:
            if path.exists():
                with path.open() as f:
                    self._config.update(toml.load(f))

    def session(self, warn: bool = False, **kwargs: Any) -> Session:
        sess = Session(warn=warn)
        self(sess, **kwargs)
        return sess

    def __call__(
        self,
        sess: Session,
        ncores: int = None,
        full_restore: bool = False,
        readonly: bool = False,
    ) -> None:
        self._plugins = {
            'parallel': Parallel(ncores),
            'tmpdir': TmpdirManager(self._monadir / App.TMPDIR),
            'files': FileManager(self._monadir / App.FILES),
            'cache': Cache.from_path(
                self._monadir / App.CACHE, full_restore=full_restore, readonly=readonly
            ),
        }
        for plugin in self._plugins.values():
            plugin(sess)

    def ensure_monadir(self) -> None:
        if self._monadir.is_dir():
            log.info(f'Already initialized in {self._monadir}.')
            return
        log.info(f'Initializing an empty repository in {self._monadir}.')
        self._monadir.mkdir()
        try:
            cache_home = Path(self._config['cache'])
        except KeyError:
            for dirname in [App.TMPDIR, App.FILES]:
                (self._monadir / dirname).mkdir()
        else:
            ts = get_timestamp()
            cachedir = cache_home / f'{Path.cwd().name}_{ts}'
            cachedir.mkdir()
            for dirname in [App.TMPDIR, App.FILES]:
                (cachedir / dirname).mkdir()
                (self._monadir / dirname).symlink_to(cachedir / dirname)
