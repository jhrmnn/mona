# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
from typing_extensions import Protocol, runtime

from .errors import InvalidInput
from .files import File
from .rules import Rule
from .runners import run_process
from .sessions import Session
from .utils import Pathable, make_executable

__version__ = '0.2.0'
__all__ = ['dir_task', 'DirtaskTmpdir']

log = logging.getLogger(__name__)

DirtaskInput = Union[File, Tuple[Path, str]]


@runtime
class TmpdirManager(Protocol):
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


class DirtaskTmpdir:
    """Context manager of a temporary directory that collects created files.

    :param output_filter: true for files to be collected
    """

    def __init__(self, output_filter: Callable[[str], bool] = None) -> None:
        sess = Session.active()
        dirmngr = cast(
            Optional[TmpdirManager], sess.storage.get('dir_task:tmpdir_manager')
        )
        assert not dirmngr or isinstance(dirmngr, TmpdirManager)
        self._dirmngr = dirmngr
        self._output_filter = output_filter

    def has_tmpdir_manager(self) -> bool:
        """Is the temporary directory managed."""
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
                self._outputs: Dict[str, File] = {}
                for path in self._tmpdir.glob('**/*'):
                    if not path.is_file():
                        continue
                    relpath = str(path.relative_to(self._tmpdir))
                    if self._output_filter and not self._output_filter(relpath):
                        continue
                    file = File.from_path(path, self._tmpdir, keep=False)
                    self._outputs[relpath] = file
        finally:
            self._ctx.__exit__(exc_type, *args)

    def result(self) -> Dict[str, File]:
        """Return a collection of files created in the temporary directory.

        This is available only after leaving the context.
        """
        return self._outputs


def checkout_files(
    root: Path,
    exe: Optional[File],
    files: Sequence[DirtaskInput],
    mutable: bool = False,
) -> None:
    assert root.exists()
    if exe:
        files = [exe, *files]
    for file in files:
        if isinstance(file, File):
            path = file.path
        else:
            path, target = file
        (root / path.parent).mkdir(parents=True, exist_ok=True)
        if isinstance(file, File):
            file.target_in(root, mutable=mutable)
        else:
            (root / path).symlink_to(target)
    if exe:
        make_executable(root / exe.path)


@Rule
async def dir_task(exe: File, inputs: List[DirtaskInput]) -> Dict[str, File]:
    """Create a rule with an executable and a files as inputs.

    The result of the task is a dictionary of all new files created by running
    the executable.
    """
    for file in [exe, *inputs]:
        if not (
            isinstance(file, File)
            or isinstance(file, list)
            and len(file) == 2
            and isinstance(file[0], Path)
            and isinstance(file[1], str)
        ):
            raise InvalidInput(str(file))
    input_names = {
        str(inp if isinstance(inp, File) else inp[0]) for inp in [exe, *inputs]
    }
    dirtask_tmpdir = DirtaskTmpdir(lambda p: p not in input_names)
    with dirtask_tmpdir as tmpdir:
        checkout_files(tmpdir, exe, inputs)
        out_path, err_path = tmpdir / 'STDOUT', tmpdir / 'STDERR'
        try:
            with out_path.open('w') as stdout, err_path.open('w') as stderr:
                await run_process(
                    str(tmpdir / exe.path), stdout=stdout, stderr=stderr, cwd=tmpdir
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
