# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import subprocess
from pathlib import Path

from typing import List

log = logging.getLogger(__name__)


class Remote:
    def __init__(self, host: str, path: str) -> None:
        self._host = host
        self._path = path

    def go(self) -> None:
        subprocess.run(
            ['ssh', '-t', self._host, f'cd {self._path} && exec $SHELL'], check=True
        )

    def update(self, *, delete: bool = False, dry: bool = False) -> None:
        log.info(f'Updating {self._host}...')
        subprocess.run(['ssh', self._host, f'mkdir -p {self._path}'], check=True)
        excludes: List[str] = ['/.mona/', '/.git/']
        excludesfiles = Path('.monaignore'), Path('.gitignore')
        for file in excludesfiles:
            if file.exists():
                with file.open() as f:
                    excludes.extend(l.strip() for l in f.readlines())
        args = ['rsync', '-cirl', *(f'--exclude={excl}' for excl in excludes)]
        if delete:
            args.append('--delete')
        if dry:
            args.append('--dry-run')
        args.append('./')
        args.append(f'{self._host}:{self._path}/')
        subprocess.run(args, check=True)
