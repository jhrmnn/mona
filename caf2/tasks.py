# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import json
from abc import abstractmethod
from typing import Any, Callable, Optional, List, TypeVar, Collection, cast, Tuple

from .futures import Future, Maybe, Empty, CafError, State
from .json import json_validate, InvalidJSON
from .hashing import Hashed, Composite, HashedCompositeLike, HashedComposite
from .utils import get_fullname

log = logging.getLogger(__name__)

_K = TypeVar('_K')
_T = TypeVar('_T')
_HFut = TypeVar('_HFut', bound='HashedFuture')  # type: ignore
_TC = TypeVar('_TC', bound='TaskComposite')


def maybe_future(obj: Any) -> Optional['HashedFuture[Any]']:
    if isinstance(obj, HashedFuture):
        return obj
    try:
        json_validate(obj, lambda x: isinstance(x, (Task, TaskComponent)))
    except InvalidJSON:
        return None
    jsonstr, components = TaskComposite.parse_object(obj)
    if any(isinstance(comp, HashedFuture) for comp in components):
        return TaskComposite(jsonstr, components)
    return None


def ensure_hashed(obj: Any) -> Hashed[Any]:
    if isinstance(obj, HashedFuture):
        return obj
    json_validate(obj, lambda x: isinstance(x, (Task, TaskComponent)))
    jsonstr, components = TaskComposite.parse_object(obj)
    if any(isinstance(comp, HashedFuture) for comp in components):
        return TaskComposite(jsonstr, components)
    return HashedComposite(jsonstr, components)


# Although this class could be hashable in principle, this would require
# dispatching all futures via a session in the same way that tasks are.
# See test_identical_futures() for an example of what wouldn't work.
class HashedFuture(Hashed[_T], Future[_T]):
    @property
    @abstractmethod
    def spec(self) -> str: ...

    @property
    @abstractmethod
    def label(self) -> str: ...

    @property
    def value(self) -> _T:
        return self.result()

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self} state={self.state.name}>'

    def __str__(self) -> str:
        return f'{self.tag}: {self.label}'


class TaskHasNotRun(CafError):
    pass


class TaskIsDone(CafError):
    pass


class Task(HashedFuture[_T]):
    def __init__(self, func: Callable[..., _T], *args: Any, label: str = None
                 ) -> None:
        self._func = func
        self._args = tuple(map(ensure_hashed, args))
        Hashed.__init__(self)
        Future.__init__(
            self, (arg for arg in self._args if isinstance(arg, HashedFuture))
        )
        self._label = label or \
            f'{self._func.__qualname__}({", ".join(a.label for a in self._args)})'
        self._future_result: Optional[HashedFuture[_T]] = None
        self._side_effects: List[Task[Any]] = []

    @property
    def spec(self) -> str:
        lines = [get_fullname(self.func)]
        lines.extend(f'{fut.hashid}  # {fut.label}' for fut in self.args)
        return '\n'.join(lines)

    @property
    def label(self) -> str:
        return self._label

    @property
    def func(self) -> Callable[..., _T]:
        return self._func

    @property
    def args(self) -> Tuple[Hashed[Any], ...]:
        return self._args

    def __getitem__(self, key: Any) -> 'TaskComponent[Any]':
        return self.get(key)

    def get(self, key: Any, default: Any = Empty._) -> 'TaskComponent[Any]':
        return TaskComponent(self, [key], default)

    @property
    def side_effects(self) -> Tuple['Task[Any]', ...]:
        return tuple(self._side_effects)

    def add_side_effect(self, task: 'Task[Any]') -> None:
        self._side_effects.append(task)

    def default_result(self) -> Maybe[_T]:
        if self._future_result:
            return self._future_result.default_result()
        return Empty._

    def set_result(self, result: _T) -> None:
        super().set_result(result)
        self._future_result = None

    def set_future_result(self, result: HashedFuture[_T]) -> None:
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


class TaskComposite(HashedCompositeLike, HashedFuture[Composite]):  # type: ignore
    def __init__(self, jsonstr: str, components: Collection[Hashed[Any]]
                 ) -> None:
        futures = [comp for comp in components if isinstance(comp, HashedFuture)]
        assert futures
        Future.__init__(self, futures)
        HashedCompositeLike.__init__(self, jsonstr, components)
        self.add_ready_callback(
            lambda self: self.set_result(self.resolve(lambda comp: comp.value))
        )

    # override abstract property in HashedCompositeLike
    value = HashedFuture.value  # type: ignore

    def default_result(self) -> Composite:
        return self.resolve(
            lambda comp:
            comp.result(check_done=False) if isinstance(comp, HashedFuture)
            else comp.value
        )


class TaskComponent(HashedFuture[_T]):
    def __init__(self, task: Task[Any], keys: List[Any],
                 default: Maybe[_T] = Empty._) -> None:
        self._task = task
        self._keys = keys
        Hashed.__init__(self)
        Future.__init__(self, [task])
        self._label = ''.join([
            self._task.label, *(f'[{k!r}]' for k in self._keys)
        ])
        self._default = default
        self.add_ready_callback(
            lambda self: self.set_result(self.resolve())
        )

    @property
    def spec(self) -> str:
        return json.dumps([self._task.hashid] + self._keys)

    @property
    def label(self) -> str:
        return self._label

    def __getitem__(self, key: Any) -> 'TaskComponent[Any]':
        return self.get(key)

    def get(self, key: Any, default: Any = Empty._) -> 'TaskComponent[Any]':
        return TaskComponent(self._task, self._keys + [key], default)

    def resolve(self) -> _T:
        obj = self._task.result()
        for key in self._keys:
            obj = obj[key]
        return cast(_T, obj)

    def default_result(self) -> Maybe[_T]:
        return self._default
