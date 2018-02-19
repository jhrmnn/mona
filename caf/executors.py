# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import subprocess
import json
import tempfile
import sys
import inspect
import pickle
import re
from textwrap import dedent
from pathlib import Path
from abc import ABC, abstractmethod

from . import Caf
from .cellar_common import Hash
from .Utils import Map

from typing import (
    Dict, Sequence, Tuple, Type, TypeVar, Generic, Union, Any, Callable
)
from typing_extensions import Protocol, runtime
from mypy_extensions import TypedDict

_U = TypeVar('_U', bound=Exception)


class Context(Protocol):
    async def task(self, execid: str, inp: bytes) -> bytes: ...


class Executor(ABC):
    name: str

    def __init__(self, app: Caf) -> None:
        self._app = app
        app.register_exec(self.name, self)

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


@runtime
class InputFile(Protocol):
    def get_hash(self) -> Hash: ...


class OutputFile(Protocol):
    def read_bytes(self) -> bytes: ...
    @property
    def path(self) -> Path: ...
    def get_hash(self) -> Hash: ...


class FileStore(Protocol[_U]):
    unfinished_exc: Type[_U]
    def save_file(self, file: Path) -> Hash: ...
    def move_file(self, file: Path) -> Hash: ...
    def save_bytes(self, contents: bytes) -> Hash: ...
    def get_file(self, hashid: Hash) -> Path: ...
    def wrap_files(self, inp: bytes, files: Dict[str, Hash]
                   ) -> Map[str, OutputFile]: ...
    def unfinished_output(self, inp: bytes) -> Map[str, OutputFile]: ...


class UnknownInputType(Exception):
    pass


class UnknownSymlinkType(Exception):
    pass


DictTask = TypedDict('DictTask', {'command': str, 'inputs': Dict[str, Hash]})
InputTarget = Union[str, Path, bytes, InputFile]
Input = Union[str, Path, Tuple[str, InputTarget]]


class DirBashExecutor(Executor, Generic[_U]):
    name = 'dir-bash'

    def __init__(self, app: Caf, store: FileStore[_U]) -> None:
        super().__init__(app)
        self._store = store

    async def create_process(self, cmd: str, **kwargs: Any
                             ) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_shell(cmd, **kwargs)

    async def __call__(self, inp: bytes) -> bytes:
        task: DictTask = json.loads(inp)
        with tempfile.TemporaryDirectory(prefix='caftsk_') as _tmpdir:
            tmpdir = Path(_tmpdir)
            for filename, hs in task['inputs'].items():
                if hs[0] == '>':
                    file = Path(hs[1:])
                else:
                    file = self._store.get_file(hs)
                (tmpdir/filename).symlink_to(file)
            with (tmpdir/'run.out').open('w') as stdout, \
                    (tmpdir/'run.err').open('w') as stderr:
                proc = await self.create_process(
                    task['command'], cwd=tmpdir, stdout=stdout, stderr=stderr,
                )
            retcode = await proc.wait()
            if retcode:
                raise subprocess.CalledProcessError(retcode, inp)
            outputs = {}
            for filepath in tmpdir.glob('**/*'):
                filename = str(filepath.relative_to(tmpdir))
                if filename not in task['inputs'] and filepath.is_file():
                    outputs[filename] = self._store.move_file(filepath)
        return json.dumps(outputs, sort_keys=True).encode()

    async def task(self, command: str,
                   inputs: Sequence[Input] = None,
                   symlinks: Sequence[Tuple[str, str]] = None,
                   label: str = None,
                   ) -> Map[str, OutputFile]:
        hashed_inputs: Dict[str, Hash] = {}
        file: InputTarget
        for item in inputs or []:
            if isinstance(item, str):
                path, file = item, Path(item)
            elif isinstance(item, Path):
                path, file = str(item), item
            elif isinstance(item, tuple) and len(item) == 2 \
                    and isinstance(item[0], str):
                path, file = item
            else:
                raise UnknownInputType(item)
            path = str(Path(path))  # normalize
            if isinstance(file, (str, Path)):
                hashed_inputs[path] = self._store.save_file(Path(file))
            elif isinstance(file, bytes):
                hashed_inputs[path] = self._store.save_bytes(file)
            else:
                hashed_inputs[path] = file.get_hash()
        for target, source in symlinks or []:
            if not isinstance(target, str) and not isinstance(source, str):
                raise UnknownSymlinkType((target, source))
            hashed_inputs[str(Path(target))] = Hash('>' + str(Path(source)))
        dict_inp = {'command': command, 'inputs': hashed_inputs}
        inp = json.dumps(dict_inp, sort_keys=True).encode()
        try:
            out = await self._app.task(self.name, inp, label)
        except self._store.unfinished_exc:
            return self._store.unfinished_output(inp)
        return self._store.wrap_files(inp, json.loads(out))


class DirPythonExecutor(DirBashExecutor[_U]):
    name = 'dir-python'

    async def create_process(self, cmd: str, **kwargs: Any
                             ) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            sys.executable, '_exec.py', **kwargs
        )

    def function_task(self, func: Callable[..., Any]
                      ) -> Callable[..., Any]:
        func_code = inspect.getsource(func).split('\n', 1)[1]
        func_code = re.sub(r'\s*#.*$', '', func_code, flags=re.MULTILINE)
        signature = inspect.signature(func)
        positional = [
            p.name for p in signature.parameters.values() if p.default is p.empty
        ]

        async def task(*args: InputTarget, label: str = None, **kwargs: Any
                       ) -> Any:
            assert len(args) == len(positional)
            arglist = ', '.join(repr(p) for p in positional)
            for kw, val in kwargs.items():
                arglist += f', {kw}={val!r}'
            task_code = dedent(
                """\
                import pickle

                {func_code}
                result = {func_name}({arglist})
                with open('_result.pickle', 'bw') as f:
                    pickle.dump(result, f)"""
            ).format(
                func_code=func_code,
                func_name=func.__name__,
                arglist=arglist,
            )
            inputs = list(zip(positional, args))
            inputs.append(('_exec.py', task_code.encode()))
            outputs = await super(DirPythonExecutor, self).task(
                'python3 _exec.py', inputs, label=label
            )
            result = pickle.loads(outputs['_result.pickle'].read_bytes())
            if result is None:
                return outputs
            return result
        return task
