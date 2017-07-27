# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
import os
import inspect
import pickle
from textwrap import dedent

from .Logging import error
from .Cellar import get_hash, State, TaskObject, Cellar, Configuration

from typing import (  # noqa
    NamedTuple, Dict, Tuple, Set, Optional, Union, List, cast, Any, Callable,
    NewType, Type, Sequence, Iterable
)
from .Cellar import Hash, TPath  # noqa


Contents = NewType('Contents', str)


class Target:
    def __init__(self, path: Path, task: 'Task') -> None:
        self.path = path
        self.task = task
        self.children = {path.name: task}
        task.parents.append(self)

    def __repr__(self) -> str:
        return f"<Target '{self.path}'>"

    def __str__(self) -> str:
        return str(self.path.parent) if len(self.path.parts) > 1 else ''


class UnknownInputType(Exception):
    pass


class UnknownSymlinkType(Exception):
    pass


class MalformedTask(Exception):
    pass


InputTarget = Union[Path, Contents, Tuple[str, 'VirtualFile']]
Input = Union[str, Path, Tuple[str, InputTarget]]
TaskFeature = Callable[[Dict[str, Any]], None]


class Task:
    tasks: Dict[Hash, 'Task'] = {}

    def __init__(
            self, *,
            command: str,
            inputs: Sequence[Input],
            symlinks: Sequence[Tuple[str, str]],
            ctx: 'Context'
    ) -> None:
        self.obj = TaskObject(command)
        file: InputTarget
        for item in inputs:
            if isinstance(item, str):
                path, file = item, Path(item)
            elif isinstance(item, Path):
                path, file = str(item), item
            elif isinstance(item, tuple) and len(item) == 2 \
                    and isinstance(item[0], (str, Path)):
                path, file = item
            else:
                raise UnknownInputType(item)
            path = str(Path(path))  # normalize
            if isinstance(file, Path):
                self.obj.inputs[path] = ctx.get_source(file)
            elif isinstance(file, str):
                self.obj.inputs[path] = ctx.store_text(file)
            elif isinstance(file, tuple) and len(file) == 2 \
                    and isinstance(file[0], str) \
                    and isinstance(file[1], VirtualFile):
                childname, vfile = file
                self.obj.children[childname] = vfile.task.hashid
                self.obj.childlinks[path] = (childname, vfile.name)
                vfile.task.parents.append(self)
            else:
                raise UnknownInputType(item)
        for target, source in symlinks:
            if not isinstance(target, str) and not isinstance(source, str):
                raise UnknownSymlinkType((target, source))
            self.obj.symlinks[str(Path(target))] = str(Path(source))
        self.hashid: Hash = self.obj.hashid
        Task.tasks[self.hashid] = self
        self.parents: List[Union[Target, Task]] = []
        self.ctx = ctx

    def __hash__(self) -> int:
        return hash(self.hashid)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Task):
            return NotImplemented  # type: ignore
        return self.hashid == other.hashid

    def __repr__(self) -> str:
        return f'<Task obj={self.obj!r} hash={self.hashid!r} parents={self.parents!r}>'

    def __str__(self) -> str:
        if not self.parents:
            return '?'
        parent = self.parents[-1]
        for name, child in parent.children.items():
            if child == self:
                par = str(parent)
                return f'{par}/{name}' if par else name
        raise MalformedTask(repr(self))

    @property
    def children(self) -> Dict[str, 'Task']:
        return {name: Task.tasks[hashid] for name, hashid in self.obj.children.items()}

    @property
    def state(self) -> State:
        # mypy complains on direct return
        state: State = self.ctx.cellar.get_state(self.hashid)
        return state

    @property
    def finished(self) -> bool:
        return self.state == State.DONE

    @property
    def outputs(self) -> Union[Dict[str, 'StoredFile'], 'FakeOutputs']:
        taskobj = self.ctx.cellar.get_task(self.hashid)
        if not taskobj or not taskobj.outputs:
            return FakeOutputs(self)
        return {
            name: StoredFile(hashid, name, self)
            for name, hashid in taskobj.outputs.items()
        }


class VirtualFile:
    def __init__(self, name: str, task: Task) -> None:
        self.name = name
        self.task = task

    @property
    def cellarid(self) -> str:
        return f'{self.task.hashid}/{self.name}'


class StoredFile(VirtualFile, os.PathLike):
    def __init__(self, hashid: Hash, name: str, task: Task) -> None:
        super().__init__(name, task)
        self.hashid = hashid

    def __str__(self) -> str:
        return str(self.path)

    def __fspath__(self) -> str:
        return str(self)

    @property
    def path(self) -> Path:
        # mypy complains on direct return
        path: Path = self.task.ctx.cellar.get_file(self.hashid)
        return path


class FakeOutputs:
    def __init__(self, task: Task) -> None:
        self.task = task

    def __getitem__(self, name: str) -> VirtualFile:
        return VirtualFile(name, self.task)


class PickledTask(Task):
    @property
    def result(self) -> Any:
        taskobj = self.ctx.cellar.get_task(self.hashid)
        assert taskobj
        if taskobj.outputs is None:
            raise RuntimeError(f'{self!r} has no outputs')
        filehash = taskobj.outputs['_result.pickle']
        with open(self.ctx.cellar.get_file(filehash), 'rb') as f:
            return pickle.load(f)


def function_task(func: Callable) -> Callable[..., Task]:
    func_code = inspect.getsource(func).split('\n', 1)[1]
    signature = inspect.signature(func)
    positional = [p.name for p in signature.parameters.values() if p.default is p.empty]

    def task_gen(
            *args: InputTarget,
            target: TPath = None,
            ctx: 'Context',
            **kwargs: Any
    ) -> Task:
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
        inputs.append(('_exec.py', Contents(task_code)))
        return ctx(
            command='python3 _exec.py',
            inputs=inputs,
            target=target,
            klass=PickledTask,
        )
    return task_gen


def base_feature(task: Dict[str, Any]) -> None:
    task.setdefault('inputs', [])
    task.setdefault('symlinks', [])


class Context:
    """Represent a build configuration: tasks and targets."""

    def __init__(self, top: Path, cellar: Cellar, conf_only: bool = False) -> None:
        self.top = top
        self.cellar = cellar
        self.tasks: List[Task] = []
        self.targets: Dict[Path, Target] = {}
        self.inputs: Dict[Hash, Union[str, bytes]] = {}
        self._sources: Dict[Path, Hash] = {}
        self.conf_only = conf_only

    def __call__(
            self, *,
            target: Union[TPath, str] = None,
            klass: Type[Task] = Task,
            features: List[TaskFeature] = None,
            **kwargs: Any
    ) -> Task:
        features = [base_feature, *(features or [])]
        for feature in features:
            feature(kwargs)
        task = klass(ctx=self, **kwargs)
        if target:
            path = Path(target)
            if path in self.targets:
                error(f'Multiple definitions of target {target!r}')
            targetnode = Target(path, task)
            self.targets[path] = targetnode
        self.tasks.append(task)
        return task

    def get_source(self, path: Path) -> Hash:
        if path in self._sources:
            return self._sources[path]
        try:
            content = Contents(path.read_text())
            hashid = self.store_text(content)
        except UnicodeDecodeError:
            content_bytes = path.read_bytes()
            hashid = self.store_bytes(content_bytes)
        self._sources[path] = hashid
        return hashid

    def store_text(self, content: Contents) -> Hash:
        hashid = get_hash(content)
        if hashid not in self.inputs:
            self.inputs[hashid] = content
        return hashid

    def store_bytes(self, content: bytes) -> Hash:
        hashid = get_hash(content)
        if hashid not in self.inputs:
            self.inputs[hashid] = content
        return hashid

    def get_configuration(self) -> Configuration:
        return Configuration(
            {task.hashid: task.obj for task in self.tasks},
            {
                TPath(str(target.path)): target.task.hashid
                for target in self.targets.values()
            },
            self.inputs,
            {task.hashid: TPath(str(task)) for task in self.tasks}
        )
