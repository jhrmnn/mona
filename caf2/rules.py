# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from abc import abstractmethod
from typing import Any, Callable, TypeVar, Generic, Dict, Union, TYPE_CHECKING
from typing_extensions import Protocol, runtime

from .utils import make_executable
from .tasks import Task
from .sessions import Session
from .hashing import Hash
from .errors import InvalidFileTarget

_T = TypeVar('_T')
HashedPathOrBytes = Union['HashedPath', bytes]
if TYPE_CHECKING:
    PathLike = os.PathLike[str]
else:
    from os import PathLike


class HashedPath(PathLike):
    @property
    @abstractmethod
    def hashid(self) -> Hash: ...


@runtime
class FileManager(Protocol):
    def store_path(self, path: Path) -> HashedPath: ...


class Rule(Generic[_T]):
    def __init__(self, func: Callable[..., _T]) -> None:
        self._func = func

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        return Session.active().create_task(self.func, *args, **kwargs)

    @property
    def func(self) -> Callable[..., _T]:
        return self._func


class HookedRule(Rule[_T]):
    def __init__(self, func: Callable[..., _T], hook: str) -> None:
        Rule.__init__(self, func)
        self._hook = hook

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        hooks = Session.active().storage.get(f'hook:{self._hook}')
        if hooks:
            pre_hook, post_hook = hooks
            args = pre_hook(args)
        task = Rule.__call__(self, *args, **kwargs)
        if hooks:
            task.add_hook(post_hook)
        return task


def with_hook(name: str) -> Callable[[Rule[_T]], HookedRule[_T]]:
    def decorator(rule: Rule[_T]) -> HookedRule[_T]:
        return HookedRule(rule.func, name)
    return decorator


@with_hook('dir_task')
@Rule
def dir_task(exe: Union[HashedPath, bytes],
             inputs: Dict[str, Union[HashedPath, bytes, Path]]
             ) -> Union[Dict[str, bytes], Dict[str, HashedPath]]:
    inputs = {'EXE': exe, **inputs}
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        exefile = str(root/'EXE')
        for filename, target in inputs.items():
            if isinstance(target, bytes):
                (root/filename).write_bytes(target)
            elif isinstance(target, (HashedPath, Path)):
                (root/filename).symlink_to(Path(target))
            else:
                raise InvalidFileTarget(repr(target))
        make_executable(exefile)
        with (root/'STDOUT').open('w') as stdout, \
                (root/'STDERR').open('w') as stderr:
            subprocess.run(
                [exefile], stdout=stdout, stderr=stderr, cwd=root, check=True,
            )
        outputs = {}
        fmngr = Session.active().storage.get('dir_task:file_manager')
        assert not fmngr or isinstance(fmngr, FileManager)
        for path in root.glob('**/*'):
            relpath = path.relative_to(root)
            if str(relpath) not in inputs and path.is_file():
                outputs[str(relpath)] = \
                    fmngr.store_path(path) if fmngr else path.read_bytes()
    return outputs
