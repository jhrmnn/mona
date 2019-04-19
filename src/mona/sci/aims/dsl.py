from importlib import resources
from typing import Any, Dict, cast

from textx.metamodel import TextXClass, metamodel_from_str  # type: ignore

from .. import aims

__version__ = '0.1.0'
__all__ = ()

_bools = {'.true.': True, '.false.': False}
_aims_mm = metamodel_from_str(
    resources.read_text(aims, 'aims.tx'),
    match_filters={
        'FLOAT_': lambda s: float(s.replace('d', 'e')),
        'LebedevInt': int,
        'BOOL_': lambda s: _bools[s],
    },
    auto_init_attributes=False,
)


def _model_to_dict(o: Any) -> Any:
    if isinstance(o, TextXClass):
        return {
            k: _model_to_dict(v)
            for k, v in vars(o).items()
            if k[0] != '_' and k != 'parent'
        }
    if isinstance(o, list):
        return [_model_to_dict(x) for x in o]
    return o


def expand_dicts(o: Any) -> Any:
    if isinstance(o, dict):
        return tuple(map(expand_dicts, o.values()))
    if isinstance(o, list):
        return list(map(expand_dicts, o))
    return o


def parse_aims_input(source: str) -> Dict[str, Any]:
    model = _aims_mm.model_from_str(source)
    return cast(Dict[str, Any], _model_to_dict(model))
