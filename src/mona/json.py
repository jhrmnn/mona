# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from pathlib import PosixPath
from typing import (
    Any,
    Callable,
    Dict,
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

from .dag import traverse_id
from .errors import CompositeError

__all__ = ()

_T = TypeVar('_T')
# JSONContainer should be Union[List[JSONValue], Dict[str, JSONValue]]
JSONContainer = NewType('JSONContainer', object)
JSONValue = Union[None, bool, int, float, str, JSONContainer]
JSONConverter = Callable[[_T], Dict[str, JSONValue]]
JSONAdapter = Callable[[Dict[str, JSONValue]], _T]
JSONDefault = Callable[[object], Optional[Tuple[object, str, Dict[str, JSONValue]]]]
JSONHook = Callable[[str, Dict[str, JSONValue]], Union[_T, Dict[str, JSONValue]]]
ClassRegister = Dict[Type[object], Tuple[JSONConverter[Any], JSONAdapter[object]]]

registered_classes: ClassRegister = {
    PosixPath: (
        lambda p: {'path': str(p)},
        lambda dct: PosixPath(cast(str, dct['path'])),
    )
}


def validate_json(obj: object, hook: Callable[[object], bool] = None) -> None:
    classes = tuple(registered_classes)

    def parents(o: object) -> Iterable[object]:
        if o is None or isinstance(o, (str, int, float, bool)):
            return ()
        if isinstance(o, classes):
            return ()
        if hook and hook(o):
            return ()
        elif isinstance(o, list):
            return o
        elif isinstance(o, dict):
            for key in o:
                if not isinstance(key, str):
                    raise CompositeError('Dict keys must be strings')
            return o.values()
        else:
            raise CompositeError(f'Unknown object: {o!r}')

    for _ in traverse_id([obj], parents):
        pass


class ClassJSONEncoder(json.JSONEncoder):
    def __init__(
        self, *args: Any, tape: Set[object], default: JSONDefault, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._default = default
        self._tape = tape
        self._classes = tuple(registered_classes)
        self._default_encs = {
            cls: enc for cls, (enc, dec) in registered_classes.items()
        }

    def default(self, o: object) -> JSONValue:
        type_tag: Optional[str] = None
        if isinstance(o, self._classes):
            dct = self._default_encs[o.__class__](o)
            type_tag = o.__class__.__name__
        else:
            encoded = self._default(o)
            if encoded is not None:
                o, type_tag, dct = encoded
                self._tape.add(o)
        if type_tag is not None:
            return cast(JSONContainer, {'_type': type_tag, **dct})
        return cast(JSONValue, super().default(o))


class ClassJSONDecoder(json.JSONDecoder):
    def __init__(self, *args: Any, hook: JSONHook[object], **kwargs: Any) -> None:
        assert 'object_hook' not in kwargs
        kwargs['object_hook'] = self._my_object_hook
        super().__init__(*args, **kwargs)
        self._hook = hook
        self._default_decs = {
            cls.__name__: dec for cls, (enc, dec) in registered_classes.items()
        }

    def _my_object_hook(self, dct: Dict[str, JSONValue]) -> object:
        try:
            type_tag = dct.pop('_type')
            assert isinstance(type_tag, str)
        except KeyError:
            return dct
        if type_tag in self._default_decs:
            return self._default_decs[type_tag](dct)
        return self._hook(type_tag, dct)
