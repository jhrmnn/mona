# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import base64
from pathlib import Path

from typing import Any, Set, Type, Dict, Callable


class ClassJSONEncoder(json.JSONEncoder):
    defaults_classes = {
        bytes: lambda b: {'bytes': base64.b64encode(b).decode()},
        Path: lambda p: {'path': str(p)},
    }

    def __init__(self, *args: Any, tape: Set[Any] = None,
                 classes: Dict[Type[Any], Callable[[Any], Dict[Any, Any]]] = None,
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

    def default(self, o: Any) -> Any:
        if isinstance(o, self._all_classes_tuple):
            if isinstance(o, self._classes_tuple) and self._tape is not None:
                self._tape.add(o)
            class_ = o.__class__
            return {'__classname__': class_.__name__, **self._classes[class_](o)}
        return super().default(o)


class ClassJSONDecoder(json.JSONDecoder):
    def __init__(self, *args: Any,
                 classes: Dict[Type[Any], Callable[[Dict[str, Any]], Any]] = None,
                 **kwargs: Any) -> None:
        assert 'object_hook' not in kwargs
        kwargs['object_hook'] = self._my_object_hook
        super().__init__(*args, **kwargs)
        classes = {
            bytes: lambda dct: base64.b64decode(dct['bytes'].encode()),
            Path: lambda dct: Path(dct['path']),
            **(classes or {})
        }
        self._classes = {
            class_.__name__: f for class_, f in (classes or {}).items()
        }

    def _my_object_hook(self, o: Dict[str, Any]) -> Any:
        try:
            classname = o.pop('__classname__')
        except KeyError:
            return o
        return self._classes[classname](o)
