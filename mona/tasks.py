# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import json
from abc import abstractmethod
import asyncio
import inspect
import pickle
from typing import (
    Callable,
    Optional,
    List,
    TypeVar,
    cast,
    Tuple,
    Union,
    Awaitable,
    Dict,
    Iterable,
)

from .futures import Future, State
from .hashing import (
    Hashed,
    Composite,
    HashedCompositeLike,
    HashedComposite,
    hash_function,
    HashResolver,
)
from .utils import get_fullname, Maybe, Empty, swap_type, import_fullname
from .errors import FutureError, TaskError, CompositeError

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_T_co = TypeVar('_T_co', covariant=True)
_U = TypeVar('_U')
Corofunc = Callable[..., Awaitable[_T]]


def ensure_hashed(obj: object) -> Hashed[object]:
    """Turn any object into a Hashed object.

    Return Hashed objects without change. Wraps composites into
    a TaskComposite or a HashedComposite. Raises InvalidJSONObject when
    not possible.
    """
    obj = swap_type(obj, TaskComposite.type_swaps)
    if isinstance(obj, Hashed):
        return obj
    jsonstr, components = TaskComposite.parse_object(obj)
    if any(isinstance(comp, HashedFuture) for comp in components):
        return TaskComposite(jsonstr, components)
    return HashedComposite(jsonstr, components)


def maybe_hashed(obj: object) -> Optional[Hashed[object]]:
    """Wraps maybe_hashed() with return value None on error."""
    try:
        return ensure_hashed(obj)
    except CompositeError:
        return None


# Although this class could be hashable in principle, this would require
# dispatching all futures via a session in the same way that tasks are.
# See test_identical_futures() for an example of what wouldn't work.
class HashedFuture(Hashed[_T_co], Future):
    """
    Represents a hashed future.

    Inherits abstract methods spec() and label() from Hashed, implements
    abstract property value and adds abstract method result().
    """

    @property
    @abstractmethod
    def spec(self) -> bytes:
        ...

    @property
    @abstractmethod
    def label(self) -> str:
        ...

    @abstractmethod
    def result(self) -> _T_co:
        ...

    @property
    def value(self) -> _T_co:
        if self.done():
            return self.result()
        raise FutureError(f'Not done: {self!r}', self)

    def default_result(self) -> _T_co:
        raise FutureError(f'No default: {self!r}', self)

    @property
    def value_or_default(self) -> _T_co:
        if self.done():
            return self.result()
        return self.default_result()

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self} state={self.state.name}>'


class Task(HashedFuture[_T_co]):
    def __init__(
        self,
        corofunc: Corofunc[_T_co],
        *args: object,
        label: str = None,
        default: Maybe[_T_co] = Empty._,
    ) -> None:
        self._corofunc = corofunc
        self._args = tuple(map(ensure_hashed, args))
        Hashed.__init__(self)
        Future.__init__(
            self, (arg for arg in self._args if isinstance(arg, HashedFuture))
        )
        self._default = default
        self._label = label or (
            f'{self._corofunc.__qualname__}'
            f'({", ".join(a.label for a in self._args)})'
        )
        self._result: Union[_T_co, Hashed[_T_co], Empty] = Empty._
        self._hook: Optional[Callable[[_T_co], _T_co]] = None
        self._storage: Dict[str, object] = {}

    @property
    def spec(self) -> bytes:
        return json.dumps(
            [
                get_fullname(self._corofunc),
                hash_function(self._corofunc),
                *(fut.hashid for fut in self._args),
            ]
        ).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'Task[_T_co]':
        rule_name, corohash, *arg_hashes = json.loads(spec)
        corofunc = import_fullname(rule_name).corofunc
        assert inspect.iscoroutinefunction(corofunc)
        assert hash_function(corofunc) == corohash
        args = (resolve(h) for h in arg_hashes)
        return cls(corofunc, *args)

    @property
    def label(self) -> str:
        return self._label

    def result(self) -> _T_co:
        return self.resolve(lambda res: res.value)

    @property
    def corofunc(self) -> Corofunc[_T_co]:
        return self._corofunc

    @property
    def args(self) -> Tuple[Hashed[object], ...]:
        return self._args

    @property
    def storage(self) -> Dict[str, object]:
        return self._storage

    def __getitem__(self, key: object) -> 'TaskComponent[object]':
        return self.get(key)

    def get(
        self, key: object, default: Maybe[object] = Empty._
    ) -> 'TaskComponent[object]':
        return TaskComponent(self, [key], default)

    def resolve(
        self, handler: Callable[[Hashed[_T_co]], _U] = None
    ) -> Union[_U, _T_co]:
        if isinstance(self._result, Empty):
            raise TaskError(f'Has not run: {self!r}', self)
        if not isinstance(self._result, Hashed):
            return self._result
        handler = handler or (lambda x: x)  # type: ignore
        return handler(self._result)  # type: ignore

    def default_result(self) -> _T_co:
        if not isinstance(self._default, Empty):
            return self._default
        if isinstance(self._result, HashedFuture):
            return cast(HashedFuture[_T_co], self._result).default_result()
        raise TaskError(f'Has no defualt: {self!r}', self)

    def metadata(self) -> Optional[bytes]:
        return pickle.dumps((self._default, self._label))

    def set_metadata(self, metadata: bytes) -> None:
        self._default, self._label = pickle.loads(metadata)

    def set_running(self) -> None:
        assert self._state is State.READY
        self._state = State.RUNNING

    def set_error(self) -> None:
        assert self._state is State.RUNNING
        self._state = State.ERROR

    def set_has_run(self) -> None:
        assert self._state is State.RUNNING
        self._state = State.HAS_RUN

    def set_result(self, result: Union[_T_co, Hashed[_T_co]]) -> None:
        assert self._state is State.HAS_RUN
        assert not isinstance(result, HashedFuture) or result.done()
        self._result = result
        self.set_done()

    def set_future_result(self, result: HashedFuture[_T_co]) -> None:
        assert self.state is State.HAS_RUN
        assert not result.done()
        self._state = State.AWAITING
        self._result = result

    def future_result(self) -> HashedFuture[_T_co]:
        if self._state < State.AWAITING:
            raise TaskError(f'Do not have future: {self!r}', self)
        if self._state > State.AWAITING:
            raise TaskError(f'Already done: {self!r}', self)
        assert isinstance(self._result, HashedFuture)
        return self._result

    def call(self) -> _T_co:
        return asyncio.run(self.call_async())

    async def call_async(self) -> _T_co:
        args = [
            arg.value_or_default if isinstance(arg, HashedFuture) else arg.value
            for arg in self.args
        ]
        return await self._corofunc(*args)


class TaskComponent(HashedFuture[_T_co]):
    def __init__(
        self, task: Task[object], keys: List[object], default: Maybe[_T_co] = Empty._
    ) -> None:
        self._task = task
        self._keys = keys
        Hashed.__init__(self)
        Future.__init__(self, [cast(HashedFuture[object], task)])
        self._default = default
        self._label = ''.join([self._task.label, *(f'[{k!r}]' for k in self._keys)])
        self.add_ready_callback(lambda self: self.set_done())

    @property
    def spec(self) -> bytes:
        return json.dumps([self._task.hashid, *self._keys]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'TaskComponent[_T_co]':
        task_hash, *keys = json.loads(spec)
        task = cast(Task[object], resolve(task_hash))
        return cls(task, keys)

    @property
    def label(self) -> str:
        return self._label

    def result(self) -> _T_co:
        return self.resolve(lambda task: task.result())

    def __getitem__(self, key: object) -> 'TaskComponent[object]':
        return self.get(key)

    def get(
        self, key: object, default: Maybe[object] = Empty._
    ) -> 'TaskComponent[object]':
        return TaskComponent(self._task, self._keys + [key], default)

    @property
    def task(self) -> Task[object]:
        return self._task

    def resolve(self, handler: Callable[[Task[object]], object]) -> _T_co:
        obj = handler(self._task)
        for key in self._keys:
            obj = obj[key]  # type: ignore
        return cast(_T_co, obj)

    def default_result(self) -> _T_co:
        if not isinstance(self._default, Empty):
            return self._default
        return self.resolve(lambda task: task.default_result())

    def metadata(self) -> Optional[bytes]:
        return pickle.dumps(self._default)

    def set_metadata(self, metadata: bytes) -> None:
        self._default = pickle.loads(metadata)


# the semantics may imply that the component is taken immediately after
# execution, but it is only taken by the child task, so that if the component
# does not exist, the exception is raised only later
class TaskComposite(HashedCompositeLike, HashedFuture[Composite]):  # type: ignore
    def __init__(self, jsonstr: str, components: Iterable[Hashed[object]]) -> None:
        components = list(components)
        futures = [comp for comp in components if isinstance(comp, HashedFuture)]
        assert futures
        Future.__init__(self, futures)
        HashedCompositeLike.__init__(self, jsonstr, components)
        self.add_ready_callback(lambda self: self.set_done())

    # override abstract property in HashedCompositeLike
    value = HashedFuture.value  # type: ignore

    def result(self) -> Composite:
        return self.resolve(lambda comp: comp.value)

    def default_result(self) -> Composite:
        return self.resolve(
            lambda comp: cast(object, comp.value_or_default)
            if isinstance(comp, HashedFuture)
            else comp.value
        )
