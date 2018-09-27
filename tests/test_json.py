import pytest  # type: ignore

import json

from caf2.json import ClassJSONDecoder, ClassJSONEncoder, validate_json, \
    InvalidJSONObject


class K:
    def __init__(self, x):
        self.x = x

    def __hash__(self):
        return hash(self.x)

    def __eq__(self, other):
        return self.x == other.x


def test_encoder_decoder():
    obj = {'x': K(1), 'ys': [K(2)]}
    tape = set()
    jsonstr = json.dumps(
        obj,
        tape=tape,
        default=lambda x:
        (x, 'K', {'x': x.x}) if isinstance(x, K) else None,
        cls=ClassJSONEncoder
    )
    assert len(tape) == 2
    obj2 = json.loads(
        jsonstr,
        hook=lambda type_tag, dct:
        K(dct['x']) if type_tag == 'K' else dct,
        cls=ClassJSONDecoder
    )
    assert obj == obj2


def test_validation_errors():
    with pytest.raises(InvalidJSONObject):
        validate_json({1: 2})
    with pytest.raises(InvalidJSONObject):
        validate_json({"1": object()})


def test_encoding_errors():
    with pytest.raises(TypeError):
        json.dumps(
            [object()],
            tape=set(),
            default=lambda x: None,
            cls=ClassJSONEncoder
        )
