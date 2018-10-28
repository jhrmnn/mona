# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import shutil
import logging
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from abc import ABC, abstractmethod
from typing import (
    TypeVar,
    Dict,
    Union,
    ContextManager,
    Any,
    Optional,
    Mapping,
    Callable,
)
from typing_extensions import Final

from ..utils import make_executable, Pathable
from ..sessions import Session
from ..hashing import HashedBytes
from ..errors import InvalidInput
from ..rules import Rule, with_hook
from ..runners import run_process

__version__ = '0.1.1'

log = logging.getLogger(__name__)

_T = TypeVar('_T')
DirTaskResult = Dict[str, HashedBytes]

EXE_NAME: Final = 'EXE'


class HashingPath(ABC):
    """Represents a path that guarantees immutability."""

    @property
    @abstractmethod
    def path(self) -> Path:
        """the actual path"""
        ...


class FileManager(ABC):
    @abstractmethod
    def store_from_path(self, path: Path) -> HashedBytes:
        ...


class TmpdirManager(ABC):
    @abstractmethod
    def tempdir(self) -> ContextManager[Pathable]:
        ...


class DirTaskProcessError(subprocess.CalledProcessError):
    def __init__(self, stdout: bytes, stderr: bytes, *args: Any) -> None:
        super().__init__(*args)
        self.stdout = stdout
        self.stderr = stderr

    def __str__(self) -> str:
        return '\n'.join(
            [
                'STDOUT:',
                self.stdout.decode(),
                '',
                'STDERR:',
                self.stderr.decode(),
                '',
                super().__str__(),
            ]
        )


def symlink_from(src: Union[str, Path], dst: Path) -> None:
    dst.symlink_to(src)


def copy_from(src: Path, dst: Path) -> None:
    shutil.copyfile(src, dst)


def checkout_files(
    root: Path,
    exe: Union[HashingPath, bytes],
    files: Mapping[str, Union[bytes, Path, HashingPath]],
    copy: bool = False,
) -> None:
    files = {EXE_NAME: exe, **files}
    target_from = copy_from if copy else symlink_from
    for filename, target in files.items():
        if isinstance(target, bytes):
            (root / filename).write_bytes(target)
        elif isinstance(target, Path):
            symlink_from(target, root / filename)
        elif isinstance(target, HashingPath):
            target_from(target.path, root / filename)
        else:
            raise InvalidInput(f'Invalid target {target!r}')
    make_executable(root / EXE_NAME)


class DirtaskTmpdir:
    """
    Context manager of a temporary directory that collects created files.

    :param output_filter: true for files to be collected
    """

    def __init__(self, output_filter: Callable[[str], bool] = None) -> None:
        sess = Session.active()
        fmngr = sess.storage.get('dir_task:file_manager')
        assert not fmngr or isinstance(fmngr, FileManager)
        self._fmngr = fmngr
        dirmngr = sess.storage.get('dir_task:tmpdir_manager')
        assert not dirmngr or isinstance(dirmngr, TmpdirManager)
        self._dirmngr = dirmngr
        self._output_filter = output_filter

    def has_tmpdir_manager(self) -> bool:
        return self._dirmngr is not None

    def __enter__(self) -> Path:
        self._ctx = (
            TemporaryDirectory() if not self._dirmngr else self._dirmngr.tempdir()
        )
        self._tmpdir = Path(self._ctx.__enter__())
        return self._tmpdir

    def __exit__(self, exc_type: Any, *args: Any) -> None:
        try:
            if not exc_type:
                self._outputs: DirTaskResult = {}
                for path in self._tmpdir.glob('**/*'):
                    if not path.is_file():
                        continue
                    relpath = path.relative_to(self._tmpdir)
                    if self._output_filter and not self._output_filter(str(relpath)):
                        continue
                    if self._fmngr:
                        output = self._fmngr.store_from_path(path)
                    else:
                        output = HashedBytes(path.read_bytes())
                    self._outputs[str(relpath)] = output
        finally:
            self._ctx.__exit__(exc_type, *args)

    def result(self) -> DirTaskResult:
        """
        The collection of files created in the temporary directory. This is
        available only after leaving the context.
        """
        return self._outputs


@with_hook('dir_task')
@Rule
async def dir_task(
    exe: Union[HashingPath, bytes], inputs: Dict[str, Union[HashingPath, bytes, Path]]
) -> DirTaskResult:
    """
    Task rule with an executable and a collection of files as inputs and a
    collection of output files as output.
    """
    dirtask_tmpdir = DirtaskTmpdir(lambda p: p != EXE_NAME and p not in inputs)
    with dirtask_tmpdir as tmpdir:
        checkout_files(tmpdir, exe, inputs)
        out_path, err_path = tmpdir / 'STDOUT', tmpdir / 'STDERR'
        try:
            with out_path.open('w') as stdout, err_path.open('w') as stderr:
                await run_process(
                    str(tmpdir / EXE_NAME), stdout=stdout, stderr=stderr, cwd=tmpdir
                )
        except subprocess.CalledProcessError as e:
            if dirtask_tmpdir.has_tmpdir_manager():
                raise
            exc: Optional[subprocess.CalledProcessError] = e
        else:
            exc = None
        if exc:
            out = out_path.read_bytes()
            err = err_path.read_bytes()
            raise DirTaskProcessError(out, err, exc.returncode, exc.cmd)
    return dirtask_tmpdir.result()
