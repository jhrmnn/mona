# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import logging
import hashlib
from enum import Enum
from contextlib import contextmanager
from abc import ABC, abstractmethod

from .json_utils import ClassJSONEncoder, ClassJSONDecoder

from typing import Iterable, Set, Any, NewType, Dict, Callable, Optional, \
    List, Deque, TypeVar, Union, Iterator, overload, Collection, Generic, \
    cast, Type
from typing import Mapping  # noqa

log = logging.getLogger(__name__)

_T = TypeVar('_T')
Callback = Callable[[_T], None]
Hash = NewType('Hash', str)
_Fut = TypeVar('_Fut', bound='Future')  # type: ignore
_HFut = TypeVar('_HFut', bound='HashedFuture')  # type: ignore


class CafError(Exception):
    pass


def hash_text(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


class NoResult(Enum):
    TOKEN = 0


class State(Enum):
    PENDING = 0
    READY = 1
    RUNNING = 2
    HAS_RUN = 3
    DONE = 1


_NoResult = NoResult.TOKEN
Maybe = Union[_T, NoResult]


class FutureNotDone(CafError):
    pass


class Future(Generic[_T]):
    def __init__(self: _Fut, parents: Iterable['Future[Any]']) -> None:
        self._pending: Set['Future[Any]'] = set()
        for fut in parents:
            if not fut.done():
                self._pending.add(fut)
        self._children: Set['Future[Any]'] = set()
        self._result: Maybe[_T] = _NoResult
        self._done_callbacks: List[Callback[_Fut]] = []
        self._ready_callbacks: List[Callback[_Fut]] = []
        self._registered = False

    def register(self: _Fut) -> _Fut:
        assert not self._registered
        self._registered = True
        for fut in self._pending:
            fut.add_child(self)
        return self

    def ready(self) -> bool:
        return not self._pending

    def done(self) -> bool:
        return self._result is not _NoResult

    @property
    def state(self) -> State:
        if self.done():
            return State.DONE
        if self.ready():
            return State.READY
        return State.PENDING

    def add_child(self, fut: 'Future[Any]') -> None:
        self._children.add(fut)

    def add_ready_callback(self: _Fut, callback: Callback[_Fut]) -> None:
        if self.ready():
            callback(self)
        else:
            self._ready_callbacks.append(callback)

    def add_done_callback(self: _Fut, callback: Callback[_Fut]) -> None:
        assert not self.done()
        self._done_callbacks.append(callback)

    def parent_done(self: _Fut, fut: 'Future[Any]') -> None:
        self._pending.remove(fut)
        if self.ready():
            log.debug(f'{self}: ready')
            for callback in self._ready_callbacks:
                callback(self)

    def default_result(self, default: _T) -> _T:
        return default

    def result(self, default: Maybe[_T] = _NoResult) -> _T:
        if not isinstance(self._result, NoResult):  # mypy limitation
            return self._result
        if not isinstance(default, NoResult):  # mypy limitation
            return self.default_result(default)
        raise FutureNotDone(repr(self))

    def set_result(self: _Fut, result: _T) -> None:
        assert self.ready()
        assert self._result is _NoResult
        self._result = result
        log.debug(f'{self}: done')
        for fut in self._children:
            fut.parent_done(self)
        for callback in self._done_callbacks:
            callback(self)


class HashedFuture(Future[_T], ABC):
    @property
    @abstractmethod
    def hashid(self) -> Hash: ...

    @property
    @abstractmethod
    def spec(self) -> str: ...

    def __repr__(self) -> str:
        return (
            f'<{self.__class__.__name__} hashid={self.hashid} spec={self.spec!r} '
            f'state={self.state.name}>'
        )

    def __str__(self) -> str:
        return self.hashid

    def register(self: _HFut) -> _HFut:
        super().register()
        log.debug(f'registered: {self!r}')
        return self


class Template(HashedFuture[_T]):
    def __init__(self, jsonstr: str, futures: Collection[HashedFuture[Any]]
                 ) -> None:
        super().__init__(futures)
        self._jsonstr = jsonstr
        self._futures = {fut.hashid: fut for fut in futures}
        self._hashid = Hash(f'{{}}{hash_text(self._jsonstr)}')
        self.add_ready_callback(
            lambda tmpl: tmpl.set_result(tmpl.substitute())
        )

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def spec(self) -> str:
        return self._jsonstr

    def has_futures(self) -> bool:
        return bool(self._futures)

    def substitute(self, default: Maybe[_T] = _NoResult) -> _T:
        return cast(_T, json.loads(
            self._jsonstr,
            classes={
                Task: lambda dct: self._futures[dct['hashid']].result(default),
                Indexor: lambda dct: self._futures[dct['hashid']].result(default),
            },
            cls=ClassJSONDecoder
        ))

    default_result = substitute

    @classmethod
    def from_object(cls: Type['Template[_T]'], obj: _T) -> 'Template[_T]':
        futures: Set[HashedFuture[Any]] = set()
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


class Indexor(HashedFuture[_T]):
    def __init__(self, task: 'Task[Mapping[Any, Any]]', keys: List[Any]) -> None:
        super().__init__([task])
        self._task = task
        self._keys = keys
        self._hashid = Hash('/'.join(['@' + task.hashid, *map(str, keys)]))
        self.add_ready_callback(
            lambda idx: idx.set_result(idx.resolve())
        )

    def __getitem__(self, key: Any) -> 'Indexor[Any]':
        return Indexor(self._task, self._keys + [key]).register()

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def spec(self) -> str:
        return self._hashid

    def resolve(self) -> _T:
        obj = self._task.result()
        for key in self._keys:
            obj = obj[key]
        return cast(_T, obj)


def wrap_output(obj: _T) -> Union[_T, Future[_T]]:
    if isinstance(obj, Future):
        return obj
    template = Template.from_object(obj)
    if template.has_futures():
        return template.register()
    return obj


class Task(HashedFuture[_T]):
    def __init__(self, f: Callable[..., _T], *args: HashedFuture[Any],
                 default: Maybe[_T] = _NoResult, label: str = None) -> None:
        super().__init__(args)
        self._f = f
        self._args = args
        self._hashid = hash_text(self.spec)
        self.children: List['Task'[Any]] = []
        self._future_result: Maybe[Future[_T]] = _NoResult
        self._default = default
        self._label = label

    def __getitem__(self, key: Any) -> Indexor[Any]:
        return Indexor(self, [key]).register()  # type: ignore

    def default_result(self, default: Any) -> _T:
        if not isinstance(self._future_result, NoResult):
            return self._future_result.default_result(default)
        return super().default_result(default)

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
        return self._future_result is not _NoResult

    def run(self, allow_unfinished: bool = False) -> Union[_T, Future[_T]]:
    @property
    def state(self) -> State:
        state = super().state
        if state is State.READY and self.has_run():
            state = State.HAS_RUN
        return state

        assert not self.done()
        if not allow_unfinished:
            assert self.ready()
        log.debug(f'{self}: will run')
        args = [arg.result(self._default) for arg in self._args]
        result = wrap_output(self._f(*args))
        if self.children:
            log.info(f'{self}: created children: {[c.hashid for c in self.children]}')
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


class NoActiveSession(CafError):
    pass


class ArgNotInSession(CafError):
    pass


class Session:
    _active: Optional['Session'] = None

    def __init__(self) -> None:
        self._pending: Set[Task[Any]] = set()
        self._waiting = Deque[Task[Any]]()
        self._tasks: Dict[Hash, Task[Any]] = {}
        self._task_tape: Optional[List[Task[Any]]] = None

    def __enter__(self) -> 'Session':
        assert Session._active is None
        Session._active = self
        return self

    def __exit__(self, *args: Any) -> None:
        Session._active = None
        self._pending.clear()
        self._waiting.clear()
        self._tasks.clear()

    def _schedule_task(self, task: Task[Any]) -> None:
        self._pending.remove(task)
        self._waiting.append(task)

    def create_task(self, f: Callable[..., _T], *args: Any, **kwargs: Any
                    ) -> Task[_T]:
        fut_args = []
        for arg in args:
            if isinstance(arg, Task):
                if arg.hashid not in self._tasks:
                    raise ArgNotInSession(repr(arg))
            if isinstance(arg, HashedFuture):
                fut_args.append(arg)
            else:
                fut_args.append(Template.from_object(arg).register())
        task = Task(f, *fut_args, **kwargs)
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
    def record(self, tape: List[Task[Any]]) -> Iterator[None]:
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
        if cls._active is None:
            raise NoActiveSession()
        return cls._active


class Rule(Generic[_T]):
    def __init__(self, func: Callable[..., _T], **kwargs: Any) -> None:
        self._func = func
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'<Rule func={self._func!r} kwargs={self._kwargs!r}>'

    def __call__(self, *args: Any, **kwargs: Any) -> Task[_T]:
        return Session.active().create_task(
            self._func, *args, **self._kwargs, **kwargs
        )


@overload
def rule(func: Callable[..., _T]) -> Rule[_T]: ...
@overload  # noqa
def rule(*, label: str = None, default: Any = None
         ) -> Callable[[Callable[..., _T]], Rule[_T]]: ...


def rule(*args: Callable[..., _T], **kwargs: Any
         ) -> Union[Rule[_T], Callable[[Callable[..., _T]], Rule[_T]]]:
    if args:
        assert not kwargs
        func, = args
        return Rule(func)

    def decorator(func: Callable[..., _T]) -> Rule[_T]:
        return Rule(func, **kwargs)
    return decorator


def get_fullname(obj: Any) -> str:
    return f'{obj.__module__}:{obj.__qualname__}'
