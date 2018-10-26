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
    CAFDIR = '.caf'
    TMPDIR = 'tmpdir'
    FILES = 'files'
    CACHE = 'cache.db'

    def __init__(self, cafdir: Pathable = None) -> None:
        cafdir = cafdir or os.environ.get('CAF_DIR') or App.CAFDIR
        self._cafdir = Path(cafdir).resolve()
        self._config: Dict[str, Any] = {}
        for path in [
                Path('~/.config/caf/config.toml').expanduser(),
                Path('caf.toml'),
                self._cafdir/'config.toml',
        ]:
            if path.exists():
                with path.open() as f:
                    self._config.update(toml.load(f))

    def session(self, **kwargs: Any) -> Session:
        sess = Session()
        self(sess, **kwargs)
        return sess

    def __call__(self, sess: Session, ncores: int = None,
                 full_restore: bool = False) -> None:
        self._plugins = {
            'parallel': Parallel(ncores),
            'tmpdir': TmpdirManager(self._cafdir/App.TMPDIR),
            'files': FileManager(self._cafdir/App.FILES),
            'cache': Cache.from_path(
                self._cafdir/App.CACHE,
                full_restore=full_restore,
            ),
        }
        for plugin in self._plugins.values():
            plugin(sess)

    def ensure_cafdir(self) -> None:
        if self._cafdir.is_dir():
            log.info(f'Already initialized in {self._cafdir}.')
            return
        log.info(f'Initializing an empty repository in {self._cafdir}.')
        self._cafdir.mkdir()
        try:
            cache_home = Path(self._config['cache'])
        except KeyError:
            for dirname in [App.TMPDIR, App.FILES]:
                (self._cafdir/dirname).mkdir()
        else:
            ts = get_timestamp()
            cachedir = cache_home/f'{Path.cwd().name}_{ts}'
            cachedir.mkdir()
            for dirname in [App.TMPDIR, App.FILES]:
                (cachedir/dirname).mkdir()
                (self._cafdir/dirname).symlink_to(cachedir/dirname)
