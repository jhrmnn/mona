# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from collections import deque
import logging
import hashlib
from contextlib import contextmanager
from abc import ABC, abstractmethod

from .json_utils import ClassJSONEncoder, ClassJSONDecoder

from typing import Iterable, Set, Any, NewType, Dict, Callable, Optional, \
    List, Deque, TypeVar, Union, Iterator, overload, Collection

log = logging.getLogger(__name__)

Hash = NewType('Hash', str)
_Fut = TypeVar('_Fut', bound='Future')
_HFut = TypeVar('_HFut', bound='HashedFuture')
CallbackFut = Callable[[_Fut], None]


def hash_text(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


class FutureNotDone(Exception):
    pass


class Future:
    def __init__(self, parents: Iterable['Future']) -> None:
        self._pending: Set['Future'] = set()
        for fut in parents:
            if not fut.done():
                self._pending.add(fut)
        self._children: Set['Future'] = set()
        self._result: Any = FutureNotDone
        self._done_callbacks: List[CallbackFut] = []
        self._ready_callbacks: List[CallbackFut] = []

    def register(self: _Fut) -> _Fut:
        for fut in self._pending:
            fut.add_child(self)
        return self

    def ready(self) -> bool:
        return not self._pending

    def done(self) -> bool:
        return self._result is not FutureNotDone

    def add_child(self, fut: 'Future') -> None:
        self._children.add(fut)

    def add_ready_callback(self, callback: CallbackFut) -> None:
        if self.ready():
            callback(self)
        else:
            self._ready_callbacks.append(callback)

    def add_done_callback(self, callback: CallbackFut) -> None:
        assert not self.done()
        self._done_callbacks.append(callback)

    def parent_done(self, fut: 'Future') -> None:
        self._pending.remove(fut)
        if self.ready():
            log.debug(f'{self}: ready')
            for callback in self._ready_callbacks:
                callback(self)

    def default_result(self, default: Any) -> Any:
        return default

    def result(self, default: Any = FutureNotDone) -> Any:
        if self._result is not FutureNotDone:
            return self._result
        if default is not FutureNotDone:
            return self.default_result(default)
        raise FutureNotDone()

    def set_result(self, result: Any) -> None:
        assert self.ready()
        assert self._result is FutureNotDone
        self._result = result
        log.debug(f'{self}: done')
        for fut in self._children:
            fut.parent_done(self)
        for callback in self._done_callbacks:
            callback(self)


class HashedFuture(Future, ABC):
    @property
    @abstractmethod
    def hashid(self) -> Hash: ...

    @property
    @abstractmethod
    def spec(self) -> str: ...

    def __repr__(self) -> str:
        return self.hashid

    def register(self: _HFut) -> _HFut:
        super().register()
        log.debug(f'{self} <= {self.spec}')
        return self


class Template(HashedFuture):
    def __init__(self, jsonstr: str, futures: Collection[HashedFuture]) -> None:
        super().__init__(futures)
        self._jsonstr = jsonstr
        self._futures = {fut.hashid: fut for fut in futures}
        self._hashid = Hash(f'{{}}{ hash_text(self._jsonstr)}')
        self.add_ready_callback(
            lambda tmpl: tmpl.set_result(tmpl.substitute())  # type: ignore
        )

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def spec(self) -> str:
        return self._jsonstr

    def has_futures(self) -> bool:
        return bool(self._futures)

    def substitute(self, default: Any = FutureNotDone) -> Any:
        return json.loads(
            self._jsonstr,
            classes={
                Task: lambda dct: self._futures[dct['hashid']].result(default),
                Indexor: lambda dct: self._futures[dct['hashid']].result(default),
            },
            cls=ClassJSONDecoder
        )

    default_result = substitute

    @classmethod
    def from_object(cls, obj: Any) -> 'Template':
        futures: Set[HashedFuture] = set()
        jsonstr = json.dumps(
            obj,
            sort_keys=True,
            tape=futures,
            classes={
                Task: lambda fut: {'hashid': fut.hashid},
                Indexor: lambda fut: {'hashid': fut.hashid},
            },
            cls=ClassJSONEncoder
        )
        return cls(jsonstr, futures)


class Indexor(HashedFuture):
    def __init__(self, task: 'Task', keys: List[Union[str, int]]) -> None:
        super().__init__([task])
        self._task = task
        self._keys = keys
        self._hashid = Hash('/'.join(['@' + task.hashid, *map(str, keys)]))
        self.add_ready_callback(
            lambda idx: idx.set_result(idx.resolve())  # type: ignore
        )

    def __getitem__(self, key: Union[str, int]) -> 'Indexor':
        return Indexor(self._task, self._keys + [key]).register()

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def spec(self) -> str:
        return self._hashid

    def resolve(self) -> Any:
        obj = self._task.result()
        for key in self._keys:
            obj = obj[key]
        return obj


def wrap_input(obj: Any) -> Future:
    if isinstance(obj, Future):
        return obj
    return Template.from_object(obj).register()


def wrap_output(obj: Any) -> Any:
    if isinstance(obj, Future):
        return obj
    template = Template.from_object(obj)
    if template.has_futures():
        return template.register()
    return obj


class Task(HashedFuture):
    def __init__(self, f: Callable, *args: HashedFuture,
                 default: Any = None, label: str = None) -> None:
        super().__init__(args)
        self._f = f
        self._args = args
        self._hashid = hash_text(self.spec)
        self.children: List['Task'] = []
        self._future_result: Any = FutureNotDone
        self._default = default
        self._label = label

    def __getitem__(self, key: Union[str, int]) -> Indexor:
        return Indexor(self, [key]).register()

    def default_result(self, default: Any) -> Any:
        if self._future_result is not FutureNotDone:
            return self._future_result.default_result(default)
        super().default_result(default)

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def spec(self) -> str:
        obj = [get_fullname(self._f), *(fut.hashid for fut in self._args)]
        return json.dumps(obj, sort_keys=True)

    @property
    def label(self) -> Optional[str]:
        return self._label

    def has_run(self) -> bool:
        return self._future_result is not FutureNotDone

    def run(self, allow_unfinished: bool = False) -> Any:
        assert not self.done()
        if not allow_unfinished:
            assert self.ready()
        log.debug(f'{self}: will run')
        args = [arg.result(self._default) for arg in self._args]
        result = wrap_output(self._f(*args))
        if self.children:
            log.info(f'{self}: created children: {self.children}')
        if not self.ready():
            return result
        if isinstance(result, Future):
            assert not result.done()
            self._future_result = result
            log.info(f'{self}: has run, pending: {result}')
            result.add_done_callback(lambda fut: self.set_result(fut.result()))
        else:
            self.set_result(result)
        return result


class Session:
    _active: Optional['Session'] = None

    def __init__(self) -> None:
        self._pending: Set[Task] = set()
        self._waiting: Deque[Task] = deque()
        self._tasks: Dict[Hash, Task] = {}
        self._task_tape: Optional[List[Task]] = None

    def __enter__(self) -> 'Session':
        assert Session._active is None
        Session._active = self
        return self

    def __exit__(self, *args: Any) -> None:
        Session._active = None
        self._pending.clear()
        self._waiting.clear()
        self._tasks.clear()

    def _schedule_task(self, task: Task) -> None:
        self._pending.remove(task)
        self._waiting.append(task)

    def create_task(self, f: Callable, *args: Any, **kwargs: Any) -> Task:
        args = tuple(map(wrap_input, args))
        task = Task(f, *args, **kwargs)
        try:
            return self._tasks[task.hashid]
        except KeyError:
            pass
        task.register()
        self._pending.add(task)
        if self._task_tape is not None:
            self._task_tape.append(task)
        task.add_ready_callback(self._schedule_task)
        self._tasks[task.hashid] = task
        return task

    @contextmanager
    def record(self, tape: List[Task]) -> Iterator[None]:
        self._task_tape = tape
        try:
            yield
        finally:
            self._task_tape = None

    def eval(self, obj: Any) -> Any:
        if isinstance(obj, Future):
            fut = obj
        else:
            template = Template.from_object(obj)
            if not template.has_futures():
                return obj
            fut = template.register()
        while self._waiting:
            task = self._waiting.popleft()
            if task.done():
                continue
            with self.record(task.children):
                task.run()
        return fut.result()

    @classmethod
    def active(cls) -> 'Session':
        assert cls._active is not None
        return cls._active


class Rule:
    def __init__(self, func: Callable, **kwargs: Any) -> None:
        self._func = func
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'<Rule func={self._func!r} kwargs={self._kwargs!r}>'

    def __call__(self, *args: Any, **kwargs: Any) -> Task:
        return Session.active().create_task(
            self._func, *args, **self._kwargs, **kwargs
        )


@overload
def rule(func: Callable) -> Rule: ...
@overload  # noqa
def rule(*, label: str = None, default: Any = None
         ) -> Callable[[Callable], Rule]: ...


def rule(*args: Callable, **kwargs: Any) -> Any:
    if args:
        assert not kwargs
        func, = args
        return Rule(func)

    def decorator(func: Callable) -> Rule:
        return Rule(func, **kwargs)
    return decorator


def get_fullname(obj: Any) -> str:
    return f'{obj.__module__}:{obj.__qualname__}'
