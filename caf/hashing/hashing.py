# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import hashlib
from abc import ABC, abstractmethod
from typing import (
    NewType,
    Union,
    Generic,
    TypeVar,
    Dict,
    cast,
    Iterable,
    Set,
    Callable,
    Tuple,
    Optional,
)

from ..json import ClassJSONEncoder, ClassJSONDecoder, JSONValue, validate_json
from ..utils import Literal, shorten_text, TypeSwaps, swap_type

__version__ = '0.1.0'

_T_co = TypeVar('_T_co', covariant=True)
Hash = NewType('Hash', str)
# symbolic type for a JSON-like container including custom classes
Composite = NewType('Composite', object)
HashResolver = Callable[[Hash], 'Hashed[object]']


def hash_text(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


class Hashed(ABC, Generic[_T_co]):
    def __init__(self) -> None:
        assert not hasattr(self, '_hashid')
        self._hashid = hash_text(self.spec)

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

    def metadata(self) -> Optional[bytes]:
        return None

    def set_metadata(self, metadata: bytes) -> None:
        raise NotImplementedError

    def __str__(self) -> str:
        return f'{self.tag}: {self.label}'

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self}>'

    @property
    def hashid(self) -> Hash:
        return self._hashid

    @property
    def tag(self) -> str:
        return self.hashid[:6]


class HashedBytes(Hashed[bytes]):
    def __init__(self, content: bytes) -> None:
        self._content = content
        Hashed.__init__(self)
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


class HashedCompositeLike(Hashed[Composite]):
    type_swaps: TypeSwaps = {bytes: HashedBytes}

    def __init__(self, jsonstr: str, components: Iterable[Hashed[object]]) -> None:
        self._jsonstr = jsonstr
        self._components = {comp.hashid: comp for comp in components}
        Hashed.__init__(self)
        self._label = repr(self.resolve(lambda hashed: Literal(hashed.label)))

    @property
    @abstractmethod
    def value(self) -> Composite:
        ...

    @property
    def spec(self) -> bytes:
        return json.dumps([self._jsonstr, *sorted(self._components)]).encode()

    @classmethod
    def from_spec(cls, spec: bytes, resolve: HashResolver) -> 'HashedCompositeLike':
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

        obj = json.loads(self._jsonstr, hook=hook, cls=ClassJSONDecoder)
        return cast(Composite, obj)

    @classmethod
    def _default(cls, o: object) -> Optional[Tuple[object, str, Dict[str, JSONValue]]]:
        o = swap_type(o, cls.type_swaps)
        if isinstance(o, Hashed):
            return (o, 'Hashed', {'hashid': o.hashid})
        return None

    @classmethod
    def parse_object(cls, obj: object) -> Tuple[str, Set[Hashed[object]]]:
        classes = (Hashed,) + tuple(cls.type_swaps)
        validate_json(obj, lambda x: isinstance(x, classes))
        components: Set[Hashed[object]] = set()
        jsonstr = json.dumps(
            obj,
            sort_keys=True,
            tape=components,
            default=cls._default,
            cls=ClassJSONEncoder,
        )
        return jsonstr, components


class HashedComposite(HashedCompositeLike):
    def __init__(self, jsonstr: str, components: Iterable[Hashed[object]]) -> None:
        HashedCompositeLike.__init__(self, jsonstr, components)
        self._value = self.resolve(lambda comp: comp.value)

    @property
    def value(self) -> Composite:
        return self._value
