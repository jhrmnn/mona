# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from contextlib import contextmanager
import json
from collections import deque
import logging
import hashlib
from abc import ABC, abstractmethod

from .json_utils import ClassJSONEncoder, ClassJSONDecoder

from typing import Iterable, Set, Any, NewType, Dict, Callable, Optional, \
    List, Iterator, Deque, TypeVar, Generic, Union

log = logging.getLogger(__name__)

Hash = NewType('Hash', str)
_T = TypeVar('_T')
_F = TypeVar('_F', bound='Future', contravariant=True)
CallbackF = Callable[[_F], None]


def get_hash(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


class FutureNotDone(Exception):
    pass


class Future(ABC, Generic[_F]):
    def __init__(self, deps: Iterable['Future']) -> None:
        self._pending: Set['Future'] = set()
        for fut in deps:
            if not fut.done():
                self._pending.add(fut)
                fut.add_depant(self)
        self._depants: Set['Future'] = set()
        self._result: Any = FutureNotDone
        self._done_callbacks: List[CallbackF] = []
        self._ready_callbacks: List[CallbackF] = []

    def __repr__(self) -> str:
        return self.hashid

    def ready(self) -> bool:
        return not self._pending

    def done(self) -> bool:
        return self._result is not FutureNotDone

    def add_depant(self, fut: 'Future') -> None:
        self._depants.add(fut)

    def add_ready_callback(self, callback: CallbackF) -> None:
        if self.ready():
            callback(self)
        else:
            self._ready_callbacks.append(callback)

    def add_done_callback(self, callback: CallbackF) -> None:
        if self.done():
            callback(self)
        else:
            self._done_callbacks.append(callback)

    def dep_done(self, fut: 'Future') -> None:
        self._pending.remove(fut)
        if self.ready():
            log.debug(f'future ready: {self}')
            for callback in self._ready_callbacks:
                callback(self)

    def result(self) -> Any:
        if self._result is FutureNotDone:
            raise FutureNotDone()
        return self._result

    def set_result(self, result: Any) -> None:
        assert self._result is FutureNotDone
        self._result = result
        log.debug(f'future done: {self}')
        for fut in self._depants:
            fut.dep_done(self)
        for callback in self._done_callbacks:
            callback(self)

    @property
    @abstractmethod
    def hashid(self) -> Hash:
        ...


class Template(Future):
    def __init__(self, jsonstr: str, futures: Iterable['Future']) -> None:
        self._jstr = jsonstr
        self._hashid = Hash(f'(){get_hash(self._jstr)}')
        log.info(f'{self._hashid} <= {self._jstr}')
        super().__init__(futures)
        self._futs = {fut.hashid: fut for fut in futures}
        self.add_ready_callback(
            lambda tmpl: tmpl.set_result(tmpl.substitute()))  # type: ignore

    @property
    def hashid(self) -> Hash:
        return self._hashid

    def substitute(self) -> Any:
        def decoder(dct: Any) -> Any:
            return self._futs[dct['hashid']].result()
        return json.loads(
            self._jstr,
            classes={
                Task: decoder,
                Indexor: decoder,
            },
            cls=ClassJSONDecoder
        )

    @classmethod
    def ensure_future(cls, obj: Any) -> Any:
        if isinstance(obj, Future):
            return obj

        def encoder(fut: Future) -> Dict[str, Hash]:
            return {'hashid': fut.hashid}
        tasks: Set['Future'] = set()
        jsonstr = json.dumps(
            obj,
            sort_keys=True,
            tape=tasks,
            classes={
                Task: encoder,
                Indexor: encoder,
            },
            cls=ClassJSONEncoder
        )
        return cls(jsonstr, tasks)


class Indexor(Future):
    def __init__(self, task: 'Task', keys: List[Union[str, int]]) -> None:
        self._hashid = Hash('/'.join([task.hashid, *map(str, keys)]))
        log.info(self._hashid)
        super().__init__([task])
        self._task = task
        self._keys = keys
        self.add_ready_callback(
            lambda idx: idx.set_result(idx.resolve()))  # type: ignore

    def __getitem__(self, key: Union[str, int]) -> 'Indexor':
        return Indexor(self._task, self._keys + [key])

    @property
    def hashid(self) -> Hash:
        return self._hashid

    def resolve(self) -> Any:
        obj = self._task.result()
        for key in self._keys:
            obj = obj[key]
        return obj


class Task(Future):
    _all_tasks: Dict[Hash, 'Task'] = {}
    _register: Optional[Callable[['Task'], None]] = None

    def __init__(self, hashid: Hash, f: Callable, *args: Future) -> None:
        self._hashid = hashid
        super().__init__(args)
        self._f = f
        self._args = args
        if Task._register:
            Task._register(self)

    def __getitem__(self, key: Union[str, int]) -> Indexor:
        return Indexor(self, [key])

    @property
    def hashid(self) -> Hash:
        return self._hashid

    def run(self) -> None:
        assert self.ready()
        log.debug(f'task will run: {self}')
        args = [arg.result() for arg in self._args]
        result = Template.ensure_future(self._f(*args))
        if isinstance(result, Future):
            log.info(f'task has run, pending: {self}')
            result.add_done_callback(lambda fut: self.set_result(fut.result()))
        else:
            self.set_result(result)

    @classmethod
    def create(cls, f: Callable, *args: Any) -> 'Task':
        args = tuple(map(Template.ensure_future, args))
        hash_obj = [get_fullname(f), *(fut.hashid for fut in args)]
        hashid = get_hash(json.dumps(hash_obj, sort_keys=True))
        try:
            return cls._all_tasks[hashid]
        except KeyError:
            log.info(f'{hashid} <= {hash_obj}')
            return cls._all_tasks.setdefault(hashid, Task(hashid, f, *args))

    @classmethod
    def all_tasks(cls) -> List['Task']:
        return list(cls._all_tasks.values())

    @classmethod
    @contextmanager
    def registering(cls, register: Callable[['Task'], None]) -> Iterator[None]:
        assert cls._register is None
        cls._register = register
        try:
            yield
        finally:
            cls._register = None


class Rule:
    def __init__(self, f: Callable) -> None:
        self._f = f

    def __call__(self, *args: Any) -> Task:
        return Task.create(self._f, *args)


class Session:
    def __init__(self) -> None:
        self._pending: Set[Task] = set()
        self._waiting: Deque[Task] = deque()

    def _task_ready(self, task: Task) -> None:
        self._pending.remove(task)
        self._waiting.append(task)

    def _register_task(self, task: Task) -> None:
        self._pending.add(task)
        task.add_ready_callback(self._task_ready)

    def eval(self, task: Task) -> Any:
        Task._all_tasks.clear()  # TODO: clean this up
        self._register_task(task)
        evaled_task = task
        with Task.registering(self._register_task):
            while self._waiting:
                task = self._waiting.popleft()
                task.run()
        return evaled_task.result()


def get_fullname(obj: Any) -> str:
    return f'{obj.__module__}.{obj.__qualname__}'
