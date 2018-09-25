# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import hashlib
from abc import ABC, abstractmethod
from typing import Any, NewType, Union, Generic, TypeVar, Dict, cast, \
    Iterable, Set, Callable, Tuple, Type

from .json import ClassJSONEncoder, ClassJSONDecoder, JSONValue, validate_json
from .utils import Literal

_T = TypeVar('_T')
_HCL = TypeVar('_HCL', bound='HashedCompositeLike')
Hash = NewType('Hash', str)
# symbolic type for a JSON-like container including custom classes
Composite = NewType('Composite', object)
# HashableContainer should be Union[List[HashableValue], Dict[str, HashableValue]]
HashableContainer = NewType('HashableContainer', object)
HashableValue = Union[None, bool, int, float, str, HashableContainer]


def _hash_text(text: Union[str, bytes]) -> Hash:
    if isinstance(text, str):
        text = text.encode()
    return Hash(hashlib.sha1(text).hexdigest())


class Hashed(ABC, Generic[_T]):
    def __init__(self, hashid: Hash = None) -> None:
        self._hashid = hashid or _hash_text(self.spec)

    @property
    @abstractmethod
    def spec(self) -> Union[str, bytes]: ...

    @property
    @abstractmethod
    def label(self) -> str: ...

    @property
    @abstractmethod
    def value(self) -> _T: ...

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


class HashedCompositeLike(Hashed[Composite]):
    extra_classes: Tuple[Type[Any], ...] = ()

    def __init__(self, jsonstr: str, components: Iterable[Hashed[Any]]) -> None:
        self._jsonstr = jsonstr
        Hashed.__init__(self)
        self._components = {comp.hashid: comp for comp in components}
        self._label = repr(self.resolve(lambda hashed: Literal(hashed.label)))

    @property
    @abstractmethod
    def value(self) -> Composite:
        pass

    @property
    def spec(self) -> str:
        return self._jsonstr

    @property
    def label(self) -> str:
        return self._label

    def resolve(self, comp_handler: Callable[[Hashed[Any]], Any] = lambda x: x
                ) -> Composite:
        def hook(type_tag: str, dct: Dict[str, JSONValue]) -> Any:
            if type_tag == 'Hashed':
                return comp_handler(self._components[cast(Hash, dct['hashid'])])
            return dct
        obj = json.loads(self._jsonstr, hook=hook, cls=ClassJSONDecoder)
        return cast(Composite, obj)

    @classmethod
    def parse_object(cls, obj: HashableValue) -> Tuple[str, Set[Hashed[Any]]]:
        validate_json(obj, lambda x: isinstance(x, cls.extra_classes))
        components: Set[Hashed[Any]] = set()
        jsonstr = json.dumps(
            obj,
            sort_keys=True,
            tape=components,
            default=(
                lambda comp:
                ('Hashed', {'hashid': comp.hashid})
                if isinstance(comp, Hashed)
                else None
            ),
            cls=ClassJSONEncoder
        )
        return jsonstr, components


class HashedComposite(HashedCompositeLike):
    def __init__(self, jsonstr: str, components: Iterable[Hashed[Any]]) -> None:
        HashedCompositeLike.__init__(self, jsonstr, components)
        self._value = self.resolve(lambda comp: comp.value)

    @property
    def value(self) -> Composite:
        return self._value