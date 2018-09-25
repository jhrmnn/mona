# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import json
from abc import abstractmethod
from typing import Any, Callable, Optional, List, TypeVar, \
    Collection, cast, Tuple, Union

from .futures import Future, CafError, State
from .hashing import Hashed, Composite, HashedCompositeLike, HashedComposite
from .utils import get_fullname, Maybe, Empty
from .json import InvalidJSONObject

log = logging.getLogger(__name__)

_K = TypeVar('_K')
_T = TypeVar('_T')
_HFut = TypeVar('_HFut', bound='HashedFuture')  # type: ignore
_TC = TypeVar('_TC', bound='TaskComposite')


def ensure_hashed(obj: Any) -> Hashed[Any]:
    """Turn any object into a Hashed object.

    Returns Hashed objects without change. Wraps composites into
    a TaskComposite or a HashedComposite. Raises InvalidJSONObject when
    not possible.
    """
    obj = TaskComposite.swap_type(obj)
    if isinstance(obj, Hashed):
        return obj
    jsonstr, components = TaskComposite.parse_object(obj)
    if any(isinstance(comp, HashedFuture) for comp in components):
        return TaskComposite(jsonstr, components)
    return HashedComposite(jsonstr, components)


def maybe_hashed(obj: Any) -> Optional['Hashed[Any]']:
    """Wraps maybe_hashed() with return value None on error."""
    try:
        return ensure_hashed(obj)
    except InvalidJSONObject:
        return None


class FutureNotDone(CafError):
    pass


class FutureHasNoDefault(CafError):
    pass


# Although this class could be hashable in principle, this would require
# dispatching all futures via a session in the same way that tasks are.
# See test_identical_futures() for an example of what wouldn't work.
class HashedFuture(Hashed[_T], Future):
    """
    Represents a hashed future.

    Inherits abstract methods spec() and label() from Hashed, implements
    abstract property value and adds abstract method get_result().
    """
    @property
    @abstractmethod
    def spec(self) -> str: ...

    @property
    @abstractmethod
    def label(self) -> str: ...

    @abstractmethod
    def get_result(self) -> _T:
        pass

    @property
    def value(self) -> _T:
        return self.result()

    def default_result(self) -> _T:
        raise FutureHasNoDefault()

    def result(self, check_done: bool = True) -> _T:
        if self.done():
            return self.get_result()
        if not check_done:
            return self.default_result()
        raise FutureNotDone(repr(self))

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self} state={self.state.name}>'


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
        self._side_effects: List[Task[Any]] = []
        self._result: Union[_T, Hashed[_T], Empty] = Empty._
        self._hook: Optional[Callable[[_T], _T]] = None

    @property
    def spec(self) -> str:
        lines = [get_fullname(self.func)]
        lines.extend(f'{fut.hashid}  # {fut.label}' for fut in self.args)
        return '\n'.join(lines)

    @property
    def label(self) -> str:
        return self._label

    def get_result(self) -> _T:
        assert not isinstance(self._result, Empty)
        if isinstance(self._result, Hashed):
            return self._result.value
        return self._result

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

    def default_result(self) -> _T:
        if isinstance(self._result, HashedFuture):
            return cast(_T, self._result.default_result())
        raise FutureHasNoDefault()

    def set_result(self, result: Union[_T, Hashed[_T]]) -> None:
        self._result = result
        super().set_done()

    def set_future_result(self, result: HashedFuture[_T]) -> None:
        assert self.state == State.READY
        self._state = State.HAS_RUN
        self._result = result

    def future_result(self) -> HashedFuture[_T]:
        if self.state is not State.HAS_RUN:
            raise TaskHasNotRun(repr(self))
        if self.done():
            raise TaskIsDone(repr(self))
        assert isinstance(self._result, HashedFuture)
        return self._result

    def call(self) -> _T:
        args = [
            arg.result(check_done=False)
            if isinstance(arg, HashedFuture)
            else arg.value
            for arg in self.args
        ]
        return self.func(*args)

    def add_hook(self, hook: Callable[[_T], _T]) -> None:
        self._hook = hook

    def hook(self, result: _T) -> _T:
        if self._hook:
            result = self._hook(result)
        return result


class TaskComponent(HashedFuture[_T]):
    def __init__(self, task: Task[Any], keys: List[Any],
                 default: Maybe[_T] = Empty._) -> None:
        self._task = task
        self._keys = keys
        Hashed.__init__(self)
        Future.__init__(self, [cast(HashedFuture[Any], task)])
        self._label = ''.join([
            self._task.label, *(f'[{k!r}]' for k in self._keys)
        ])
        self._default = default
        self.add_ready_callback(lambda self: self.set_done())

    @property
    def spec(self) -> str:
        return json.dumps([self._task.hashid] + self._keys)

    @property
    def label(self) -> str:
        return self._label

    def get_result(self) -> _T:
        return self.resolve(self._task.get_result())

    @property
    def task(self) -> Task[Any]:
        return self._task

    def __getitem__(self, key: Any) -> 'TaskComponent[Any]':
        return self.get(key)

    def get(self, key: Any, default: Any = Empty._) -> 'TaskComponent[Any]':
        return TaskComponent(self._task, self._keys + [key], default)

    def resolve(self, obj: Any) -> _T:
        for key in self._keys:
            obj = obj[key]
        return cast(_T, obj)

    def default_result(self) -> _T:
        if isinstance(self._default, Empty):
            if self._task.state is State.HAS_RUN:
                return self.resolve(self._task.future_result().default_result())
            raise FutureHasNoDefault()
        return self._default


class TaskComposite(HashedCompositeLike, HashedFuture[Composite]):  # type: ignore
    extra_classes = HashedCompositeLike.extra_classes + (Task, TaskComponent)

    def __init__(self, jsonstr: str, components: Collection[Hashed[Any]]
                 ) -> None:
        futures = [comp for comp in components if isinstance(comp, HashedFuture)]
        assert futures
        Future.__init__(self, futures)
        HashedCompositeLike.__init__(self, jsonstr, components)
        self.add_ready_callback(lambda self: self.set_done())

    # override abstract property in HashedCompositeLike
    value = HashedFuture.value  # type: ignore

    def get_result(self) -> Composite:
        return self.resolve(lambda comp: comp.value)

    def default_result(self) -> Composite:
        return self.resolve(
            lambda comp:
            comp.result(check_done=False) if isinstance(comp, HashedFuture)
            else comp.value
        )
