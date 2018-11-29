# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import inspect
import json
import logging
import pickle
from abc import abstractmethod
from typing import (
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from .errors import CompositeError, FutureError, TaskError
from .futures import Future, State
from .hashing import Composite, Hash, Hashed, HashedComposite, HashResolver
from .pyhash import hash_function
from .utils import Empty, Maybe, get_fullname, import_fullname

__all__ = ()

log = logging.getLogger(__name__)

_T = TypeVar('_T')
_T_co = TypeVar('_T_co', covariant=True)
_U = TypeVar('_U')
Corofunc = Callable[..., Awaitable[_T]]


# Although this class could be hashable in principle, this would require
# dispatching all futures via a session in the same way that tasks are.
# See test_identical_futures() for an example of what wouldn't work.
class HashedFuture(Hashed[_T_co], Future):
    """Represents a hashed future.

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
        rule: str = None,
    ) -> None:
        self._corofunc = corofunc
        self._args = tuple(map(TaskComposite.ensure_hashed, args))
        Future.__init__(
            self, (arg for arg in self._args if isinstance(arg, HashedFuture))
        )
        self._default = default
        if label:
            self._label = label
        else:
            arg_list = ', '.join(a.label for a in self._args)
            arg_list = arg_list if len(arg_list) < 50 else '...'
            self._label = f'{self._corofunc.__qualname__}({arg_list})'
        self._result: Union[_T_co, Hashed[_T_co], Empty] = Empty._
        self._storage: Dict[str, object] = {}
        self._rule = rule

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
        rule_name: str
        corohash: Hash
        arg_hashes: Tuple[Hash, ...]
        rule_name, corohash, *arg_hashes = json.loads(spec)
        corofunc: Corofunc[_T_co] = getattr(import_fullname(rule_name), 'corofunc')
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
    def rule(self) -> Optional[str]:
        return self._rule

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
        return pickle.dumps((self._default, self._label, self._rule))

    def set_metadata(self, metadata: bytes) -> None:
        self._default, self._label, self._rule = pickle.loads(metadata)

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
        self,
        task: Task[object],
        keys: Iterable[object],
        default: Maybe[_T_co] = Empty._,
    ) -> None:
        self._task = task
        self._keys = list(keys)
        Future.__init__(self, [cast(HashedFuture[object], task)])
        self._default = default
        self._label = ''.join([self._task.label, *(f'[{k!r}]' for k in self._keys)])
        self.add_ready_callback(lambda self: self.set_done())

    @property
    def spec(self) -> bytes:
        return json.dumps([self._task.hashid, *self._keys]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'TaskComponent[_T_co]':
        task_hash: Hash
        keys: Tuple[object, ...]
        task_hash, *keys = json.loads(spec)
        task = cast(Task[object], resolve(task_hash))
        return cls(task, keys)

    @property
    def label(self) -> str:
        return self._label

    @property
    def components(self) -> Iterable[Hashed[object]]:
        return (self._task,)

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
class TaskComposite(HashedComposite, HashedFuture[Composite]):  # type: ignore
    def __init__(self, jsonstr: str, components: Iterable[Hashed[object]]) -> None:
        components = list(components)
        futures = [comp for comp in components if isinstance(comp, HashedFuture)]
        assert futures
        Future.__init__(self, futures)
        HashedComposite.__init__(self, jsonstr, components)
        self.add_ready_callback(lambda self: self.set_done())

    @classmethod
    def from_object(cls, obj: object) -> 'HashedComposite':
        jsonstr, components = cls.parse_object(obj)
        if any(isinstance(comp, HashedFuture) for comp in components):
            return cls(jsonstr, components)
        return HashedComposite(jsonstr, components)

    # override definition from HashedComposite
    value = HashedFuture.value  # type: ignore

    def result(self) -> Composite:
        return self.resolve(lambda comp: comp.value)

    def default_result(self) -> Composite:
        return self.resolve(
            lambda comp: cast(object, comp.value_or_default)
            if isinstance(comp, HashedFuture)
            else comp.value
        )

    @classmethod
    def ensure_hashed(cls, obj: object) -> Hashed[object]:
        """Turn any object into a Hashed object.

        Return Hashed objects without change. Wraps composites into
        a TaskComposite or a HashedComposite. Raises InvalidJSONObject when
        not possible.
        """
        obj = cls._wrap_type(obj)
        if isinstance(obj, Hashed):
            return obj
        return cls.from_object(obj)

    @classmethod
    def maybe_hashed(cls, obj: object) -> Optional[Hashed[object]]:
        """Turn any object into a Hashed object or None if not hashable."""
        try:
            return cls.ensure_hashed(obj)
        except CompositeError:
            return None
