import json

from typing import Any, Set, Type, Dict, Callable


class ClassJSONEncoder(json.JSONEncoder):
    def __init__(self, *args: Any, tape: Set[Any] = None,
                 classes: Dict[Type, Callable[[Any], Dict]] = None,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._tape = tape
        self._classes = classes or {}
        self._classes_tuple = tuple(self._classes)

    def default(self, o: Any) -> Any:
        if isinstance(o, self._classes_tuple):
            if self._tape is not None:
                self._tape.add(o)
            class_ = o.__class__
            return {'__classname__': class_.__name__, **self._classes[class_](o)}
        return super().default(o)


class ClassJSONDecoder(json.JSONDecoder):
    def __init__(self, *args: Any,
                 classes: Dict[Type, Callable[[Dict[str, Any]], Any]] = None,
                 **kwargs: Any) -> None:
        assert 'object_hook' not in kwargs
        kwargs['object_hook'] = self._my_object_hook
        super().__init__(*args, **kwargs)
        self._classes = {
            class_.__name__: f for class_, f in (classes or {}).items()
        }

    def _my_object_hook(self, o: Dict[str, Any]) -> Any:
        try:
            classname = o.pop('__classname__')
        except KeyError:
            return o
        return self._classes[classname](o)
