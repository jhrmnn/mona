# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import base64
from pathlib import Path

from typing import Any, Set, Type, Dict, Callable, overload, Sequence, \
    Union, cast
from typing_extensions import Protocol


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


class ClassJSONEncoder(json.JSONEncoder):
    defaults_classes: Dict[Type[Any], JSONConvertor] = {
        bytes: lambda b: {'bytes': base64.b64encode(b).decode()},
        Path: lambda p: {'path': str(p)},
    }

    def __init__(self, *args: Any, tape: Set[Any] = None,
                 classes: Dict[Type[Any], JSONConvertor] = None,
                 **kwargs: Any) -> None:
        classes = classes or {}
        super().__init__(*args, **kwargs)
        self._classes_tuple = tuple(classes)
        classes = {
            bytes: lambda b: {'bytes': base64.b64encode(b).decode()},
            Path: lambda p: {'path': str(p)},
            **classes
        }
        self._classes = classes
        self._all_classes_tuple = tuple(classes)
        self._tape = tape

    def default(self, o: Any) -> JSONLike:
        if isinstance(o, self._all_classes_tuple):
            if isinstance(o, self._classes_tuple) and self._tape is not None:
                self._tape.add(o)
            return {
                '__classname__': o.__class__.__name__,
                **self._classes[o.__class__](o)
            }
        return cast(JSONLike, super().default(o))


def assert_str(obj: Any) -> str:
    assert isinstance(obj, str)
    return obj


class ClassJSONDecoder(json.JSONDecoder):
    def __init__(self, *args: Any,
                 classes: Dict[Type[Any], JSONDeconvertor] = None,
                 **kwargs: Any) -> None:
        assert 'object_hook' not in kwargs
        kwargs['object_hook'] = self._my_object_hook
        super().__init__(*args, **kwargs)
        classes = {
            bytes: lambda dct: base64.b64decode(assert_str(dct['bytes']).encode()),
            Path: lambda dct: Path(assert_str(dct['path'])),
            **(classes or {})
        }
        self._classes = {
            class_.__name__: f for class_, f in (classes or {}).items()
        }

    def _my_object_hook(self, o: Dict[str, JSONLike]) -> Any:
        try:
            classname = assert_str(o.pop('__classname__'))
        except KeyError:
            return o
        return self._classes[classname](o)
