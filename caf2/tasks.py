# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import hashlib
import logging
import json
from abc import ABC, abstractmethod
from typing import Set, Any, NewType, Callable, Optional, List, TypeVar, \
    Union, Collection, cast, Tuple, Mapping, Iterable

from .futures import Future, Maybe, NoResult, CafError, State
from .json import ClassJSONEncoder, ClassJSONDecoder, validate

log = logging.getLogger(__name__)

_T = TypeVar('_T')
Hash = NewType('Hash', str)
_HFut = TypeVar('_HFut', bound='HashedFuture')  # type: ignore


def get_fullname(obj: Any) -> str:
    return f'{obj.__module__}:{obj.__qualname__}'


def hash_text(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


def shorten_text(s: str, n: int) -> str:
    if len(s) < n:
        return s
    return f'{s[:n-3]}...'


# Although this class could be hashable in principle, this would require
# dispatching all futures via a session in the same way that tasks are.
# See test_identical_futures() for an example of what wouldn't work.
class HashedFuture(Future[_T], ABC):
    def __init__(self: _HFut, parents: Iterable['HashedFuture[Any]']) -> None:
        super().__init__(parents)
        self._hashid = hash_text(self.spec)

    @property
    @abstractmethod
    def spec(self) -> str: ...

    @property
    @abstractmethod
    def label(self) -> str: ...

    @property
    def hashid(self) -> Hash:
        return self._hashid

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self} state={self.state.name}>'

    @property
    def tag(self) -> str:
        return self.hashid[:6]

    def __str__(self) -> str:
        return f'{self.tag}: {self.label}'


def ensure_future(obj: Any) -> HashedFuture[Any]:
    if isinstance(obj, HashedFuture):
        return obj
    return TaskComposite.from_object(obj)


class TaskHasNotRun(CafError):
    pass


class TaskIsDone(CafError):
    pass


class Task(HashedFuture[_T]):
    def __init__(self, func: Callable[..., _T], *args: Any, label: str = None
                 ) -> None:
        self._func = func
        self._args = tuple(map(ensure_future, args))
        if label:
            self._label = label
        else:
            self._label = self._func.__qualname__ + \
                '(' + ', '.join(a.label for a in self._args) + ')'
        super().__init__(self._args)
        self.side_effects: List[Task[Any]] = []
        self._future_result: Optional[HashedFuture[_T]] = None

    def __getitem__(self, key: Any) -> 'TaskComponent[Any]':
        return self.get(key)

    def get(self, key: Any, default: Any = NoResult._) -> 'TaskComponent[Any]':
        return TaskComponent(self, [key], default)  # type: ignore

    @property
    def func(self) -> Callable[..., _T]:
        return self._func

    @property
    def args(self) -> Tuple[HashedFuture[Any], ...]:
        return self._args

    @property
    def spec(self) -> str:
        lines = [get_fullname(self._func)]
        lines.extend(
            f'{fut.hashid}  # {shorten_text(fut.spec, 20)}' for fut in self._args
        )
        return '\n'.join(lines)

    @property
    def label(self) -> str:
        return self._label

    def default_result(self) -> Maybe[_T]:
        if self._future_result:
            return self._future_result.default_result()
        return NoResult._

    def set_result(self, result: _T, _log: bool = True) -> None:
        super().set_result(result)
        self._future_result = None

    def set_future_result(self, result: HashedFuture[Any]) -> None:
        assert self.state == State.READY
        self._state = State.HAS_RUN
        self._future_result = result

    def future_result(self) -> HashedFuture[_T]:
        if self.state is not State.HAS_RUN:
            raise TaskHasNotRun(repr(self))
        if self.done():
            assert self._future_result is None
            raise TaskIsDone(repr(self))
        assert self._future_result
        return self._future_result


class Literal(str):
    def __repr__(self) -> str:
        return self


class TaskComposite(HashedFuture[_T]):
    def __init__(self, jsonstr: str, futures: Collection[HashedFuture[Any]]
                 ) -> None:
        self._jsonstr = jsonstr
        self._futures = {fut.hashid: fut for fut in futures}
        self._label = repr(self._resolve(lambda fut: Literal(fut.label)))
        super().__init__(futures)
        if not futures:
            self.set_result(self.resolve(), _log=False)
        else:
            self.add_ready_callback(
                lambda comp: comp.set_result(comp.resolve(), _log=False)
            )

    @property
    def spec(self) -> str:
        return self._jsonstr

    @property
    def label(self) -> str:
        return self._label

    def has_futures(self) -> bool:
        return bool(self._futures)

    def _resolve(self, handler: Callable[[HashedFuture[Any]], Any]) -> _T:
        return cast(_T, json.loads(
            self._jsonstr,
            hooks={
                cls: lambda dct: handler(self._futures[dct['hashid']])
                for cls in [Task, TaskComponent]
            },
            cls=ClassJSONDecoder
        ))

    def resolve(self, check_done: bool = True) -> _T:
        return self._resolve(lambda fut: fut.result(check_done))

    def default_result(self) -> _T:
        return self.resolve(check_done=False)

    @classmethod
    def from_object(cls, obj: _T) -> 'TaskComposite[_T]':
        assert not isinstance(obj, HashedFuture)
        validate(obj, (Task, TaskComponent))
        futures: Set[HashedFuture[Any]] = set()
        jsonstr = json.dumps(
            obj,
            sort_keys=True,
            tape=futures,
            defaults={
                cls: lambda fut: {'hashid': fut.hashid}
                for cls in [Task, TaskComponent]
            },
            cls=ClassJSONEncoder
        )
        return cls(jsonstr, futures)


class TaskComponent(HashedFuture[_T]):
    def __init__(self, task: Task[Mapping[Any, Any]], keys: List[Any],
                 default: Maybe[_T] = NoResult._) -> None:
        self._task = task
        self._keys = keys
        self._label = ''.join([
            self._task.label,
            *(f'[{k!r}]' for k in self._keys)
        ])
        super().__init__([task])
        self._default = default
        self.add_ready_callback(
            lambda idx: idx.set_result(idx.resolve(), _log=False)
        )

    def __getitem__(self, key: Any) -> 'TaskComponent[Any]':
        return self.get(key)

    def get(self, key: Any, default: Any = NoResult._) -> 'TaskComponent[Any]':
        return TaskComponent(self._task, self._keys + [key], default)

    @property
    def spec(self) -> str:
        return json.dumps([self._task.hashid] + self._keys)

    @property
    def label(self) -> str:
        return self._label

    def resolve(self) -> _T:
        obj = self._task.result()
        for key in self._keys:
            obj = obj[key]
        return cast(_T, obj)

    def default_result(self) -> Maybe[_T]:
        return self._default
