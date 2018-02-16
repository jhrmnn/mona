# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import subprocess
import json
import tempfile
from pathlib import Path

from caf import Caf
from caf.ctx import Context, Task, Input
from caf.cellar import Hash, get_hash

from typing import Dict, Sequence, Tuple
from typing_extensions import Protocol
from mypy_extensions import TypedDict


class BashExecutor:
    def __init__(self, app: Caf) -> None:
        app.register_exec('bash')(self._exec)

    async def _exec(self, inp: bytes) -> bytes:
        proc = await asyncio.create_subprocess_shell(inp, stdout=asyncio.subprocess.PIPE)
        out, _ = await proc.communicate()
        if proc.returncode:
            raise subprocess.CalledProcessError(
                proc.returncode, inp,
                output=await proc.stdout.read(),  # type: ignore
            )
        return out


class FileStore(Protocol):
    def store_bytes(self, hashid: Hash, data: bytes) -> bool: ...
    def store_file(self, hashid: Hash, file: Path) -> bool: ...
    def get_file(self, hashid: Hash) -> Path: ...


DictTask = TypedDict('DictTask', {'command': str, 'inputs': Dict[str, Hash]})


class DirBashExecutor:
    def __init__(self, app: Caf, store: FileStore) -> None:
        app.register_exec('dir-bash')(self._exec)
        self._store = store

    async def _exec(self, inp: bytes) -> bytes:
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
                   )-> Dict[str, bytes]:
        task = Task(
            ctx=ctx, command=command, inputs=inputs or [],
            symlinks=symlinks or [], label=''
        )
        inp = task.obj.data
        out = await ctx.task('dir-bash', inp)
        return {
            fname: self._store.get_file(hs).read_bytes()
            for fname, hs in json.loads(out).items()
        }
