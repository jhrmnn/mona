# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import subprocess
import json
import tempfile
from pathlib import Path
from abc import ABC, abstractmethod

from . import Caf
from .ctx import Context, Task, Input
from .cellar_common import Hash, get_hash
from .Utils import Map

from typing import Dict, Sequence, Tuple, Type, TypeVar, Generic
from typing_extensions import Protocol
from mypy_extensions import TypedDict

_U = TypeVar('_U', bound=Exception)


class Executor(ABC):
    name: str

    def __init__(self, app: Caf) -> None:
        app.register_exec(self.name)(self)

    @abstractmethod
    async def __call__(self, inp: bytes) -> bytes: ...


class BashExecutor(Executor):
    name = 'bash'

    async def __call__(self, inp: bytes) -> bytes:
        proc = await asyncio.create_subprocess_shell(inp, stdout=asyncio.subprocess.PIPE)
        out, _ = await proc.communicate()
        if proc.returncode:
            raise subprocess.CalledProcessError(
                proc.returncode, inp,
                output=await proc.stdout.read(),  # type: ignore
            )
        return out


class VirtualFile(Protocol):
    def read_bytes(self) -> bytes: ...


class FileStore(Protocol[_U]):
    unfinished_exc: Type[_U]
    def store_file(self, hashid: Hash, file: Path) -> bool: ...
    def get_file(self, hashid: Hash) -> Path: ...
    def wrap_files(self, inp: bytes, files: Dict[str, Hash]
                   ) -> Map[str, VirtualFile]: ...
    def unfinished_output(self, inp: bytes) -> Map[str, VirtualFile]: ...


DictTask = TypedDict('DictTask', {'command': str, 'inputs': Dict[str, Hash]})


class DirBashExecutor(Executor, Generic[_U]):
    name = 'dir-bash'

    def __init__(self, app: Caf, store: FileStore[_U]) -> None:
        super().__init__(app)
        self._store = store

    async def __call__(self, inp: bytes) -> bytes:
        task: DictTask = json.loads(inp)
        with tempfile.TemporaryDirectory(prefix='caftsk_') as _tmpdir:
            tmpdir = Path(_tmpdir)
            for filename, hs in task['inputs'].items():
                file = self._store.get_file(hs)
                (tmpdir/filename).symlink_to(file)
            proc = await asyncio.create_subprocess_shell(task['command'], cwd=tmpdir)
            retcode = await proc.wait()
            if retcode:
                raise subprocess.CalledProcessError(retcode, inp)
            outputs = {}
            for filepath in tmpdir.glob('**/*'):
                filename = str(filepath.relative_to(tmpdir))
                if filename not in task['inputs'] and filepath.is_file():
                    hs = get_hash(filepath.read_bytes())
                    outputs[filename] = hs
                    self._store.store_file(hs, filepath)
        return json.dumps(outputs, sort_keys=True).encode()

    async def task(self, ctx: Context, command: str,
                   inputs: Sequence[Input] = None,
                   symlinks: Sequence[Tuple[str, str]] = None
                   ) -> Map[str, VirtualFile]:
        task = Task(
            ctx=ctx, command=command, inputs=inputs or [],
            symlinks=symlinks or [], label=''
        )
        inp = task.obj.data
        try:
            out = await ctx.task('dir-bash', inp)
        except self._store.unfinished_exc:
            return self._store.unfinished_output(inp)
        return self._store.wrap_files(inp, json.loads(out))
