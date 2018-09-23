# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import base64
from pathlib import Path

from typing import Any, Set, Type, Dict, Callable, overload, Sequence, \
    Union, cast, Tuple
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
    def __init__(self, *args: Any, tape: Set[Any] = None,
                 defaults: Dict[Type[Any], JSONConvertor] = None,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        defaults = defaults or {}
        self._tape_classes = tuple(defaults)
        self._defaults = {cls: enc for cls, (enc, dec) in default_classes.items()}
        self._defaults.update(defaults)
        self._classes = tuple(self._defaults)
        self._tape = tape

    def default(self, o: Any) -> JSONLike:
        if isinstance(o, self._classes):
            if self._tape is not None and isinstance(o, self._tape_classes):
                self._tape.add(o)
            return {
                '_type': o.__class__.__name__,
                **self._defaults[o.__class__](o)
            }
        return cast(JSONLike, super().default(o))


def assert_str(obj: Any) -> str:
    assert isinstance(obj, str)
    return obj


class ClassJSONDecoder(json.JSONDecoder):
    def __init__(self, *args: Any,
                 hooks: Dict[Type[Any], JSONDeconvertor] = None,
                 **kwargs: Any) -> None:
        assert 'object_hook' not in kwargs
        kwargs['object_hook'] = self._my_object_hook
        super().__init__(*args, **kwargs)
        _hooks = {cls: dec for cls, (enc, dec) in default_classes.items()}
        _hooks.update(hooks or {})
        self._hooks = {class_.__name__: f for class_, f in _hooks.items()}

    def _my_object_hook(self, o: Dict[str, JSONLike]) -> Any:
        try:
            type_tag = assert_str(o.pop('_type'))
        except KeyError:
            return o
        return self._hooks[type_tag](o)
