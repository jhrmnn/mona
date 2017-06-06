# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pathlib import Path
import json
import inspect
import pickle

from caflib.Utils import slugify
from caflib.Logging import error
from caflib.Cellar import get_hash, State, TaskObject, Cellar, get_hash_bytes

from typing import (  # noqa
    NamedTuple, Dict, Tuple, Set, Optional, Union, List, cast, Any, Callable,
    NewType, Type, Sequence
)
from caflib.Cellar import Hash, TPath  # noqa


Contents = NewType('Contents', str)


class Target:
    all_targets: Set[Path] = set()

    def __init__(self) -> None:
        self.path: Optional[Path] = None

    @property
    def children(self) -> Dict[str, 'Task']:
        assert self.path and self.task
        return {self.path.name: self.task}

    def __repr__(self) -> str:
        return f"<Target '{self.path}'>"

    def __str__(self) -> str:
        assert self.path
        return str(self.path.parent) if len(self.path.parts) > 1 else ''

    def set_task(self, task: 'Task', path: TPath) -> None:
        try:
            self.path = Path(slugify(path, path=True))
        except TypeError:
            error(f'Target path {path!r} is not a string')
        if self.path in Target.all_targets:
            error(f'Multiple definitions of target "{self.path}"')
        assert self.path
        Target.all_targets.add(self.path)
        self.task = task
        task.parents.append(self)


class UnknownInputType(Exception):
    pass


class MalformedTask(Exception):
    pass


InputTarget = Union[Path, Contents, Tuple[str, 'VirtualFile']]
Input = Union[str, Path, Tuple[str, InputTarget]]


class Task:
    tasks: Dict[Hash, 'Task'] = {}

    def __init__(
            self, *,
            command: str,
            inputs: Sequence[Input] = None,
            symlinks: Sequence[Tuple[str, str]] = None,
            ctx: 'Context'
    ) -> None:
        self.obj = TaskObject(command, {}, {}, {}, {}, {})
        file: InputTarget
        if inputs:
            for item in inputs:
                if isinstance(item, str):
                    path, file = item, Path(item)
                elif isinstance(item, Path):
                    path, file = str(item), item
                elif isinstance(item, tuple) and len(item) == 2:
                    path, file = item
                else:
                    raise UnknownInputType(item)
                if isinstance(file, Path):
                    self.obj.inputs[path] = ctx.get_source(file)
                elif isinstance(file, str):
                    self.obj.inputs[path] = ctx.store_text(file)
                elif isinstance(file, tuple):
                    childname, vfile = file
                    self.obj.children[childname] = vfile.task.hashid
                    self.obj.childlinks[path] = (childname, vfile.name)
                    vfile.task.parents.append(self)
                else:
                    raise UnknownInputType(item)
        if symlinks:
            for target, source in symlinks:
                self.obj.symlinks[target] = source
        self.hashid: Hash = get_hash(json.dumps(self.obj.asdict(), sort_keys=True))
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


class StoredFile(VirtualFile):
    def __init__(self, hashid: Hash, name: str, task: Task) -> None:
        super().__init__(name, task)
        self.hashid = hashid

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
        assert taskobj.outputs
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
        task_code = f"""\
import pickle

{func_code}
result = {func.__name__}({arglist})
with open('_result.pickle', 'bw') as f:
    pickle.dump(result, f)"""
        inputs = list(zip(positional, args))
        inputs.append(('_exec.py', Contents(task_code)))
        return ctx(
            command='python3 _exec.py',
            inputs=inputs,
            target=target,
            klass=PickledTask,
        )
    return task_gen


def get_configuration(tasks: List[Task], targets: List[Target]) \
        -> Dict[str, Union[Dict[Hash, TaskObject], Dict[str, Hash], Dict[Hash, str]]]:
    return {
        'tasks': {task.hashid: task.obj for task in tasks},
        'targets': {str(target.path): target.task.hashid for target in targets},
        'labels': {task.hashid: str(task) for task in tasks}
    }


class Context:
    """Represent a build configuration: tasks and targets."""

    def __init__(self, top: str, cellar: Cellar) -> None:
        self.top = Path(top)
        self.cellar = cellar
        self.tasks: List[Task] = []
        self.targets: List[Target] = []
        self.inputs: Dict[Hash, Union[str, bytes]] = {}
        self._sources: Dict[Path, Hash] = {}

    def __call__(
            self, *,
            target: Union[TPath, str] = None,
            klass: Type[Task] = Task,
            **kwargs: Any
    ) -> Task:
        task = klass(ctx=self, **kwargs)
        if target:
            targetnode = Target()
            self.targets.append(targetnode)
            targetnode.set_task(task, TPath(target))
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
        hashid = get_hash_bytes(content)
        if hashid not in self.inputs:
            self.inputs[hashid] = content
        return hashid
