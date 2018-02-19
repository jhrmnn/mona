from pathlib import Path
import hashlib

from textx.metamodel import metamodel_from_file  # type: ignore
from textx.metamodel import TextXClass

from typing import Dict, Any

_bools = {'.true.': True, '.false.': False}
_aims_mm = metamodel_from_file(
    Path(__file__).parent/'aims.tx',
    match_filters={
        'FLOAT_': lambda s: float(s.replace('d', 'e')),
        'LebedevInt': int,
        'BOOL_': lambda s: _bools[s],
    },
    auto_init_attributes=False
)
_parsed: Dict[str, Dict[str, Any]] = {}


def _model_to_dict(o: Any) -> Any:
    if isinstance(o, TextXClass):
        return {
            k: _model_to_dict(v) for k, v in vars(o).items()
            if k[0] != '_' and k != 'parent'
        }
    if isinstance(o, list):
        return [_model_to_dict(x) for x in o]
    return o


def parse_aims_input(control: str) -> Dict[str, Any]:
    hsh = hashlib.sha1(control.encode()).hexdigest()
    if hsh not in _parsed:
        model = _aims_mm.model_from_str(control)
        _parsed[hsh] = _model_to_dict(model)
    return _parsed[hsh]


def parse_basis(task: Dict[str, Any]) -> None:
    basis_new: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for basis_def in task['basis']:
        model = parse_aims_input(basis_def)['species'][0]
        basis_new[model['species_name']] = model
    task['basis'] = basis_new
