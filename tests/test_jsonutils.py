import json

from caf2.json import ClassJSONDecoder, ClassJSONEncoder


def test_json_utils():
    class MyClass:
        def __init__(self, x):
            self.x = x

        def __hash__(self):
            return hash(self.x)

        def __eq__(self, other):
            return self.x == other.x

    obj = {'x': MyClass(1), 'ys': [MyClass(2)]}
    tape = set()
    jsonstr = json.dumps(
        obj,
        tape=tape,
        default=lambda x:
        (x, 'MyClass', {'x': x.x}) if isinstance(x, MyClass) else None,
        cls=ClassJSONEncoder
    )
    assert len(tape) == 2
    obj2 = json.loads(
        jsonstr,
        hook=lambda type_tag, dct:
        MyClass(dct['x']) if type_tag == 'MyClass' else dct,
        cls=ClassJSONDecoder
    )
    assert obj == obj2
