# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import base64
from pathlib import Path

from typing import Any, Set, Type, Dict, Callable, overload, Sequence, \
    Union, cast, Tuple, Optional
from typing_extensions import Protocol

from .futures import CafError


class _JSONArray(Protocol):
    def __getitem__(self, idx: int) -> 'JSONLike': ...
    # hack to enforce an actual list
    def sort(self) -> None: ...


class _JSONDict(Protocol):
    def __getitem__(self, key: str) -> 'JSONLike': ...
    # hack to enforce an actual dict
    @staticmethod
    @overload
    def fromkeys(seq: Sequence[Any]) -> Dict[Any, Any]: ...
    @staticmethod
    @overload
    def fromkeys(seq: Sequence[Any], value: Any) -> Dict[Any, Any]: ...


JSONLike = Union[str, int, float, bool, None, _JSONArray, _JSONDict]
JSONConvertor = Callable[[Any], Dict[str, JSONLike]]
JSONDeconvertor = Callable[[Dict[str, JSONLike]], Any]
DefaultConvertor = Callable[[Any], Optional[Tuple[str, Dict[str, JSONLike]]]]
DefaultDeconvertor = Callable[[str, Dict[str, JSONLike]], Optional[Any]]


default_classes: Dict[Type[Any], Tuple[JSONConvertor, JSONDeconvertor]] = {
    bytes: (
        lambda b: {'bytes': base64.b64encode(b).decode()},
        lambda dct: base64.b64decode(assert_str(dct['bytes']).encode())
    ),
    Path: (
        lambda p: {'path': str(p)},
        lambda dct: Path(assert_str(dct['path']))
    )
}


class InvalidComposite(CafError):
    pass


def validate(obj: Any, extra: Tuple[Type[Any], ...] = ()) -> None:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return
    if isinstance(obj, tuple(default_classes) + extra):
        pass
    elif isinstance(obj, list):
        for x in obj:
            validate(x, extra)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                raise InvalidComposite('Dict keys must be strings')
            validate(v, extra)
    else:
        raise InvalidComposite(f'Unknown object: {obj!r}')


class ClassJSONEncoder(json.JSONEncoder):
    def __init__(self, *args: Any, tape: Set[Any], default: DefaultConvertor,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._default = default
        self._tape = tape
        self._classes = tuple(default_classes)
        self._default_encs = {
            cls: enc for cls, (enc, dec) in default_classes.items()
        }

    def default(self, o: Any) -> JSONLike:
        type_tag: Optional[str] = None
        if isinstance(o, self._classes):
            dct = self._default_encs[o.__class__](o)
            type_tag = o.__class__.__name__
        else:
            encoded = self._default(o)
            if encoded is not None:
                type_tag, dct = encoded
                self._tape.add(o)
        if type_tag is not None:
            return {'_type': type_tag, **dct}
        return cast(JSONLike, super().default(o))


def assert_str(obj: Any) -> str:
    assert isinstance(obj, str)
    return obj


class ClassJSONDecoder(json.JSONDecoder):
    def __init__(self, *args: Any, hook: DefaultDeconvertor, **kwargs: Any
                 ) -> None:
        assert 'object_hook' not in kwargs
        kwargs['object_hook'] = self._my_object_hook
        super().__init__(*args, **kwargs)
        self._hook = hook
        self._default_decs = {
            cls.__name__: dec for cls, (enc, dec) in default_classes.items()
        }

    def _my_object_hook(self, dct: Dict[str, JSONLike]) -> Any:
        try:
            type_tag = assert_str(dct.pop('_type'))
        except KeyError:
            return dct
        if type_tag in self._default_decs:
            return self._default_decs[type_tag](dct)
        return self._hook(type_tag, dct)
