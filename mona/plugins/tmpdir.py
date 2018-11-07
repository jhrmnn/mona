# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import shutil
from contextlib import contextmanager
from pathlib import Path
from tempfile import mkdtemp
from typing import Iterator

from ..dirtask import TmpdirManager as _TmpdirManager
from ..sessions import Session, SessionPlugin
from ..utils import Pathable

log = logging.getLogger(__name__)


class TmpdirManager(_TmpdirManager, SessionPlugin):
    """Plugin that manages temporary directories."""

    name = 'tmpdir_manager'

    def __init__(self, root: Pathable) -> None:
        self._root = Path(root).resolve()

    def post_enter(self, sess: Session) -> None:  # noqa: D102
        sess.storage['dir_task:tmpdir_manager'] = self

    @contextmanager
    def tempdir(self) -> Iterator[str]:  # noqa: D102
        task = Session.active().running_task
        path = mkdtemp(prefix=f'{task.hashid[:6]}_', dir=str(self._root))
        log.debug(f'Created tempdir for "{task.label}": {path}')
        try:
            yield path
        except Exception:
            raise
        else:
            shutil.rmtree(path)
