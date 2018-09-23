# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import hashlib
import logging
import json
from abc import ABC, abstractmethod
from typing import Set, Any, NewType, Callable, Optional, List, TypeVar, \
    Union, Collection, cast, Type, Tuple, Mapping

from .futures import Future, Maybe, _NoResult, CafError, State
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

    def register(self: _HFut) -> bool:
        if super().register():
            log.debug(f'registered: {self!r}')
            return True
        return False


def ensure_future(obj: Any) -> HashedFuture[Any]:
    if isinstance(obj, HashedFuture):
        return obj
    return Template.from_object(obj)


class TaskHasNotRun(CafError):
    pass


class TaskIsDone(CafError):
    pass


class Task(HashedFuture[_T]):
    def __init__(self, func: Callable[..., _T], *args: Any,
                 default: Maybe[_T] = _NoResult, label: str = None) -> None:
        self._args = tuple(map(ensure_future, args))
        super().__init__(self._args)
        self._func = func
        self._default = default  # TODO resolve this
        self._label = label
        self.children: List[Task[Any]] = []
        self._hashid = hash_text(self.spec)
        self._future_result: Optional[HashedFuture[_T]] = None

    def __getitem__(self, key: Any) -> 'Indexor[Any]':
        return Indexor(self, [key])  # type: ignore

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
        obj = [get_fullname(self._func), *(fut.hashid for fut in self._args)]
        return json.dumps(obj, sort_keys=True)

    @property
    def label(self) -> Optional[str]:
        return self._label

    @property
    def state(self) -> State:
        state = super().state
        if state is State.READY and self.has_run():
            state = State.HAS_RUN
        return state

    def default_result(self, default: Any) -> _T:
        if self._future_result:
            return self._future_result.default_result(default)
        return super().default_result(default)

    def set_result(self, result: _T) -> None:
        super().set_result(result)
        self._future_result = None

    def set_future_result(self, result: HashedFuture[Any]) -> None:
        self._future_result = result

    def future_result(self) -> HashedFuture[_T]:
        if not self.has_run():
            raise TaskHasNotRun(repr(self))
        if self.done():
            assert self._future_result is None
            raise TaskIsDone(repr(self))
        assert self._future_result
        return self._future_result

    def has_run(self) -> bool:
        return self.done() or self._future_result is not None


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
        assert not isinstance(obj, HashedFuture)
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
    def __init__(self, task: Task[Mapping[Any, Any]], keys: List[Any]) -> None:
        super().__init__([task])
        self._task = task
        self._keys = keys
        self._hashid = Hash('/'.join(['@' + task.hashid, *map(str, keys)]))
        self.add_ready_callback(
            lambda idx: idx.set_result(idx.resolve())
        )

    def __getitem__(self, key: Any) -> 'Indexor[Any]':
        return Indexor(self._task, self._keys + [key])

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
