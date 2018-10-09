# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from abc import ABC, abstractmethod
from typing import TypeVar, Dict, Union, ContextManager, Any, Optional

from ..utils import make_executable, Pathable
from ..sessions import Session
from ..hashing import HashedBytes
from ..errors import InvalidInput
from ..rules import Rule, with_hook
from ..runners import run_process

__version__ = '0.1.0'

log = logging.getLogger(__name__)

_T = TypeVar('_T')
DirTaskResult = Union[Dict[str, HashedBytes]]


class HashingPath(ABC):
    @property
    @abstractmethod
    def path(self) -> Path: ...


class FileManager(ABC):
    @abstractmethod
    def store_from_path(self, path: Path) -> HashedBytes: ...


class TmpdirManager(ABC):
    @abstractmethod
    def tempdir(self) -> ContextManager[Pathable]: ...


class DirTaskProcessError(subprocess.CalledProcessError):
    def __init__(self, stdout: bytes, stderr: bytes, *args: Any) -> None:
        super().__init__(*args)
        self.stdout = stdout
        self.stderr = stderr

    def __str__(self) -> str:
        return '\n'.join([
            'STDOUT:',
            self.stdout.decode(),
            '',
            'STDERR:',
            self.stderr.decode(),
            '',
            super().__str__()
        ])


@with_hook('dir_task')
@Rule
async def dir_task(exe: Union[HashingPath, bytes],
                   inputs: Dict[str, Union[HashingPath, bytes, Path]]
                   ) -> DirTaskResult:
    sess = Session.active()
    fmngr = sess.storage.get('dir_task:file_manager')
    assert not fmngr or isinstance(fmngr, FileManager)
    dirmngr = sess.storage.get('dir_task:tmpdir_manager')
    assert not dirmngr or isinstance(dirmngr, TmpdirManager)
    inputs = {'EXE': exe, **inputs}
    dirfactory = TemporaryDirectory if not dirmngr else dirmngr.tempdir
    with dirfactory() as tmpdir:
        root = Path(tmpdir)
        exefile = str((root/'EXE').resolve())
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
        out_path, err_path = root/'STDOUT', root/'STDERR'
        try:
            with out_path.open('w') as stdout, err_path.open('w') as stderr:
                await run_process(exefile, stdout=stdout, stderr=stderr, cwd=root)
        except subprocess.CalledProcessError as e:
            if dirmngr:
                raise
            exc: Optional[subprocess.CalledProcessError] = e
        else:
            exc = None
        if exc:
            out = out_path.read_bytes()
            err = err_path.read_bytes()
            raise DirTaskProcessError(out, err, exc.returncode, exc.cmd)
        outputs = {}
        for path in root.glob('**/*'):
            relpath = path.relative_to(root)
            if str(relpath) not in inputs and path.is_file():
                if fmngr:
                    output = fmngr.store_from_path(path)
                else:
                    output = HashedBytes(path.read_bytes())
                outputs[str(relpath)] = output
    return outputs
