# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import hashlib
import logging
import json
from abc import ABC, abstractmethod
from typing import Set, Any, NewType, Callable, Optional, List, TypeVar, \
    Union, Collection, cast, Tuple, Mapping

from .futures import Future, Maybe, NoResult, CafError, State
from .json_utils import ClassJSONEncoder, ClassJSONDecoder

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
        return self.hashid[:6]


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
        self._args = tuple(map(ensure_future, args))
        super().__init__(self._args)
        self._func = func
        self._label = label
        self.children: List[Task[Any]] = []
        self._hashid = hash_text(self.spec)
        self._future_result: Optional[HashedFuture[_T]] = None

    def __getitem__(self, key: Any) -> 'TaskComponent[Any]':
        return self.get(key)

    def get(self, key: Any, default: Any = NoResult._) -> 'TaskComponent[Any]':
        return TaskComponent(self, [key], default)  # type: ignore

    def __str__(self) -> str:
        s = super().__str__()
        if self.label is not None:
            s = f'{self.label}(s)'
        return s

    @property
    def func(self) -> Callable[..., _T]:
        return self._func

    @property
    def args(self) -> Tuple[HashedFuture[Any], ...]:
        return self._args

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def spec(self) -> str:
        lines = [get_fullname(self._func)]
        lines.extend(
            f'{fut.hashid}  # {shorten_text(fut.spec, 20)}' for fut in self._args
        )
        return '\n'.join(lines)

    @property
    def label(self) -> Optional[str]:
        return self._label

    def default_result(self) -> Maybe[_T]:
        if self._future_result:
            return self._future_result.default_result()
        return NoResult._

    def set_result(self, result: _T) -> None:
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


class TaskComposite(HashedFuture[_T]):
    def __init__(self, jsonstr: str, futures: Collection[HashedFuture[Any]]
                 ) -> None:
        super().__init__(futures)
        self._jsonstr = jsonstr
        self._futures = {fut.hashid: fut for fut in futures}
        self._hashid = hash_text(self._jsonstr)
        self.add_ready_callback(
            lambda comp: comp.set_result(comp.resolve())
        )

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def spec(self) -> str:
        return self._jsonstr

    def __str__(self) -> str:
        return f'"{shorten_text(self._jsonstr, 40)}"({super().__str__()})'

    def has_futures(self) -> bool:
        return bool(self._futures)

    def resolve(self, check_done: bool = True) -> _T:
        return cast(_T, json.loads(
            self._jsonstr,
            classes={
                cls: lambda dct: self._futures[dct['hashid']].result(check_done)
                for cls in [Task, TaskComponent]
            },
            cls=ClassJSONDecoder
        ))

    def default_result(self) -> _T:
        return self.resolve(check_done=False)

    @classmethod
    def from_object(cls, obj: _T) -> 'TaskComposite[_T]':
        assert not isinstance(obj, HashedFuture)
        futures: Set[HashedFuture[Any]] = set()
        jsonstr = json.dumps(
            obj,
            sort_keys=True,
            tape=futures,
            classes={
                Task: lambda fut: {'hashid': fut.hashid},
                TaskComponent: lambda fut: {'hashid': fut.hashid},
            },
            cls=ClassJSONEncoder
        )
        return cls(jsonstr, futures)


class TaskComponent(HashedFuture[_T]):
    def __init__(self, task: Task[Mapping[Any, Any]], keys: List[Any],
                 default: Maybe[_T] = NoResult._) -> None:
        super().__init__([task])
        self._task = task
        self._keys = keys
        self._default = default
        self._hashid = hash_text(self.spec)
        self.add_ready_callback(
            lambda idx: idx.set_result(idx.resolve())
        )

    def __getitem__(self, key: Any) -> 'TaskComponent[Any]':
        return self.get(key)

    def get(self, key: Any, default: Any = NoResult._) -> 'TaskComponent[Any]':
        return TaskComponent(self._task, self._keys + [key], default)

    @property
    def hashid(self) -> Hash:
        return self._hashid

    def _spec(self, root: str) -> str:
        return '/'.join(['@' + root, *map(str, self._keys)])

    @property
    def spec(self) -> str:
        return self._spec(self._task.hashid)

    def __str__(self) -> str:
        return self._spec(super().__str__())

    def resolve(self) -> _T:
        obj = self._task.result()
        for key in self._keys:
            obj = obj[key]
        return cast(_T, obj)

    def default_result(self) -> Maybe[_T]:
        return self._default
