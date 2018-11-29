# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import hashlib
import json
from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    NewType,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from .json import ClassJSONDecoder, ClassJSONEncoder, JSONValue, validate_json
from .utils import Literal, shorten_text

__version__ = '0.2.0'
__all__ = ()

_T = TypeVar('_T')
_T_co = TypeVar('_T_co', covariant=True)
Hash = NewType('Hash', str)
# symbolic type for a JSON-like container including custom classes
Composite = NewType('Composite', object)
HashResolver = Callable[[Hash], 'Hashed[object]']
TypeRegister = Dict[Type[object], Callable[[Any], object]]


def hash_text(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


class Hashed(ABC, Generic[_T_co]):
    @property
    @abstractmethod
    def spec(self) -> bytes:
        ...

    @classmethod
    @abstractmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'Hashed[_T_co]':
        ...

    @property
    @abstractmethod
    def label(self) -> str:
        ...

    @property
    @abstractmethod
    def value(self) -> _T_co:
        ...

    @property
    def components(self) -> Iterable['Hashed[object]']:
        """:class:`Hashed` instances required by the constructor.

        To be implemented by subclasses.
        """
        return ()

    def metadata(self) -> Optional[bytes]:
        return None

    def set_metadata(self, metadata: bytes) -> None:
        raise NotImplementedError

    def __str__(self) -> str:
        return f'{self.tag}: {self.label}'

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self}>'

    def get_hash(self) -> Hash:
        return hash_text(self.spec)

    @property
    def hashid(self) -> Hash:
        if not hasattr(self, '_hashid'):
            self._hashid = self.get_hash()
        return self._hashid

    @property
    def tag(self) -> str:
        return self.hashid[:6]


class HashedComposite(Hashed[Composite]):
    _type_register: TypeRegister = {}

    def __init__(self, jsonstr: str, components: Iterable[Hashed[object]]) -> None:
        self._jsonstr = jsonstr
        self._components = {comp.hashid: comp for comp in components}
        self._label = repr(self.resolve(lambda hashed: Literal(hashed.label)))

    @classmethod
    def from_object(cls, obj: object) -> 'HashedComposite':
        return cls(*cls.parse_object(obj))

    @property
    def value(self) -> Composite:
        if not hasattr(self, '_value'):
            self._value = self.resolve(lambda comp: comp.value)
        return self._value

    @property
    def spec(self) -> bytes:
        return json.dumps([self._jsonstr, *sorted(self._components)]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'HashedComposite':
        jsonstr: str
        hashids: Tuple[Hash, ...]
        jsonstr, *hashids = json.loads(spec)
        return cls(jsonstr, (resolve(h) for h in hashids))

    @property
    def label(self) -> str:
        return self._label

    @property
    def components(self) -> Iterable[Hashed[object]]:
        return self._components.values()

    def resolve(
        self, handler: Callable[['Hashed[object]'], object] = lambda x: x
    ) -> Composite:
        def hook(type_tag: str, dct: Dict[str, JSONValue]) -> object:
            if type_tag == 'Hashed':
                return handler(self._components[cast(Hash, dct['hashid'])])
            return dct

        return cast(
            Composite, json.loads(self._jsonstr, hook=hook, cls=ClassJSONDecoder)
        )

    @classmethod
    def _wrap_type(cls, obj: _T) -> Union[_T, Hashed[_T]]:
        if obj.__class__ in cls._type_register:
            return cast(Hashed[_T], cls._type_register[obj.__class__](obj))
        return obj

    @classmethod
    def parse_object(cls, obj: object) -> Tuple[str, Set[Hashed[object]]]:
        def default(o: object) -> Optional[Tuple[object, str, Dict[str, JSONValue]]]:
            o = cls._wrap_type(o)
            if isinstance(o, Hashed):
                return (o, 'Hashed', {'hashid': o.hashid})
            return None

        classes = tuple(cls._type_register) + (Hashed,)
        validate_json(obj, lambda x: isinstance(x, classes))
        components: Set[Hashed[object]] = set()
        jsonstr = json.dumps(
            obj, sort_keys=True, tape=components, default=default, cls=ClassJSONEncoder
        )
        return jsonstr, components

    @classmethod
    def register_type(
        cls, klass: Type[_T]
    ) -> Callable[[Type[Hashed[_T]]], Type[Hashed[_T]]]:
        def decorator(hashed_klass: Type[Hashed[_T]]) -> Type[Hashed[_T]]:
            cls._type_register[klass] = hashed_klass
            return hashed_klass

        return decorator


@HashedComposite.register_type(bytes)
class HashedBytes(Hashed[bytes]):
    def __init__(self, content: bytes) -> None:
        self._content = content
        self._label = repr(shorten_text(content, 20))

    @property
    def spec(self) -> bytes:
        return self.value

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'HashedBytes':
        return cls(spec)

    @property
    def label(self) -> str:
        return self._label

    @property
    def value(self) -> bytes:
        return self._content
