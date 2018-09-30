# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
from tempfile import TemporaryDirectory
from abc import ABC, abstractmethod
from typing import TypeVar, Dict, Union

from ..utils import make_executable
from ..sessions import Session
from ..hashing import HashedBytes
from ..errors import InvalidInput
from ..rules import Rule, with_hook
from ..runners import run_process

_T = TypeVar('_T')
DirTaskResult = Union[Dict[str, HashedBytes]]


class HashingPath(ABC):
    @property
    @abstractmethod
    def path(self) -> Path: ...


class FileManager(ABC):
    @abstractmethod
    def store_from_path(self, path: Path) -> HashedBytes: ...


@with_hook('dir_task')
@Rule
async def dir_task(exe: Union[HashingPath, bytes],
                   inputs: Dict[str, Union[HashingPath, bytes, Path]]
                   ) -> DirTaskResult:
    inputs = {'EXE': exe, **inputs}
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        exefile = str(root/'EXE')
        for filename, target in inputs.items():
            if isinstance(target, bytes):
                (root/filename).write_bytes(target)
            elif isinstance(target, (Path, HashingPath)):
                if not isinstance(target, Path):
                    target = target.path
                (root/filename).symlink_to(target)
            else:
                raise InvalidInput(f'Invalid target {target!r}')
        make_executable(exefile)
        with (root/'STDOUT').open('w') as stdout, \
                (root/'STDERR').open('w') as stderr:
            await run_process(exefile, stdout=stdout, stderr=stderr, cwd=root)
        outputs = {}
        fmngr = Session.active().storage.get('dir_task:file_manager')
        assert not fmngr or isinstance(fmngr, FileManager)
        for path in root.glob('**/*'):
            relpath = path.relative_to(root)
            if str(relpath) not in inputs and path.is_file():
                if fmngr:
                    output = fmngr.store_from_path(path)
                else:
                    output = HashedBytes(path.read_bytes())
                outputs[str(relpath)] = output
    return outputs
