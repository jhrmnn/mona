# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from .convert import p2f
from pathlib import Path
import shutil
import re

from typing import Dict, Any, List, Tuple, Callable, TypeVar, Generic

from ..Utils import Map
from ..executors import DirBashExecutor, OutputFile
from ..ctx import Context

_U = TypeVar('_U', bound=Exception)


class AimsNotFound(Exception):
    pass


Task = Dict[str, Any]


class AimsTask(Generic[_U]):
    default_features = [
        'speciedir', 'tags', 'command', 'basis', 'uncomment_tier', 'geom', 'core'
    ]

    def __init__(self, features: List[str] = None,
                 dir_bash: DirBashExecutor[_U] = None) -> None:
        self.basis_defs: Dict[Tuple[Path, str], str] = {}
        self.speciedirs: Dict[Tuple[str, str], Path] = {}
        self.features: List[Callable[[Task], None]] = [
            getattr(self, feat) for feat in features or self.default_features
        ]
        self._dir_bash = dir_bash

    def __call__(self, task: Task) -> None:
        for feature in self.features:
            feature(task)

    async def task(self, ctx: Context, **task: Any) -> Map[str, OutputFile]:
        assert self._dir_bash
        self(task)
        inputs: List[Tuple[str, bytes]] = [
            (name, contents.encode()) for name, contents in task['inputs']
        ]
        return await self._dir_bash.task(ctx, task['command'], inputs)

    def speciedir(self, task: Task) -> None:
        basis_key = aims, basis = task['aims'], task.pop('basis')
        if basis_key in self.speciedirs:
            speciedir = self.speciedirs[basis_key]
        else:
            pathname = shutil.which(aims)
            if not pathname:
                raise AimsNotFound(aims)
            path = Path(pathname)
            speciedir = path.parents[1]/'aimsfiles/species_defaults'/basis
            self.speciedirs[basis_key] = speciedir
        task['speciedir'] = speciedir

    def tags(self, task: Task) -> None:
        lines = []
        for tag, value in task.pop('tags').items():
            if value is None:
                continue
            if value is ():
                lines.append(tag)
            elif isinstance(value, list):
                lines.extend(f'{tag}  {p2f(v)}' for v in value)
            else:
                if value == 'xc' and value.startswith('libxc'):
                    lines.append('override_warning_libxc')
                lines.append(f'{tag}  {p2f(value)}')
        task['control'] = '\n'.join(lines)

    def command(self, task: Task) -> None:
        aims, check = task.pop('aims'), task.pop('check', True)
        command = f'AIMS={aims} run_aims'
        if self._dir_bash:
            command += ' >run.out 2>run.err'
        if check:
            command += ' && egrep "Have a nice day|stop_if_parser" run.out >/dev/null'
        task['command'] = command

    def basis(self, task: Task) -> None:
        speciedir = task.pop('speciedir')
        species = set([(a.number, a.specie) for a in task['geom'].centers])
        basis = []
        for number, specie in sorted(species):
            if (speciedir, specie) not in self.basis_defs:
                basis_def = (speciedir/f'{number:02d}_{specie}_default').read_text()
                self.basis_defs[speciedir, specie] = basis_def
            else:
                basis_def = self.basis_defs[speciedir, specie]
            basis.append(basis_def)
        task['basis'] = basis

    def uncomment_tier(self, task: Task) -> None:
        tier = task.pop('tier', None)
        if tier is None:
            return
        for i in range(len(task['basis'])):
            buffer = ''
            tier_now = None
            for l in task['basis'][i].split('\n'):
                m = re.search(r'"(\w+) tier"', l) or re.search(r'(Further)', l)
                if m:
                    tier_now = {'First': 1, 'Second': 2, 'Third': 3, 'Fourth': 4, 'Further': 5}[m.group(1)]
                m = re.search(r'#?(\s*(hydro|ionic) .*)', l)
                if m:
                    l = m.group(1)
                    if not (tier_now and tier_now <= tier):
                        l = '#' + l
                if '####' in l:
                    tier_now = None
                buffer += l + '\n'
            task['basis'][i] = buffer

    def geom(self, task: Task) -> None:
        task['geometry'] = task.pop('geom').dumps('aims')

    def core(self, task: Task) -> None:
        control = '\n\n'.join([task.pop('control'), *task.pop('basis')])
        task['inputs'] = [
            ('control.in', control),
            ('geometry.in', task.pop('geometry')),
        ]


def _kwid(**kw: Any) -> Dict[str, Any]:
    return kw


def _get_gaussian_basis(L: int, alpha: List[float], coeff: List[float]) -> Any:
    if len(alpha) == 1:
        return ('gaussian', L, 1, alpha[0])
    else:
        return [('gaussian', L, len(alpha)), *zip(alpha, coeff)]


class AimsWriter:
    rules: Dict[str, Callable[..., Any]] = {
        'ROOT': lambda species: species,
        'species': lambda species_name, basis, angular_grids, valence, ion_occ, **kw: [
            ('species', species_name), kw, angular_grids, valence, ion_occ, basis
        ],
        'cut_pot': lambda onset, width, scale: (onset, width, scale),
        'radial_base': lambda number, radius: (number, radius),
        'angular_grids': lambda shells: [('angular_grids', 'specified'), shells],
        'shells': lambda points, radius=None: (
            ('division', radius, points) if radius
            else ('outer_grid', points)
        ),
        'valence': lambda n, l, occupation: ('valence', n, l, occupation),
        'ion_occ': lambda n, l, occupation: ('ion_occ', n, l, occupation),
        'basis': lambda type, **kw: (
            (type, kw['n'], kw['l'], kw['radius']) if type == 'ionic'
            else (type, kw['n'], kw['l'], kw['z_eff']) if type == 'hydro'
            else _get_gaussian_basis(**kw) if type == 'gaussian'
            else None
        ),
    }

    def __init__(self, rules: Dict[str, Callable[..., Any]] = None) -> None:
        if rules:
            self.rules = rules

    def stringify(self, value: Any) -> str:
        if isinstance(value, bool):
            return f'.{str(value).lower()}.'
        if isinstance(value, tuple):
            return ' '.join(self.stringify(x) for x in value)
        if isinstance(value, list):
            return '\n'.join(self.stringify(x) for x in value)
        if isinstance(value, dict):
            return '\n'.join(
                f'{k} {self.stringify(v)}' if v is not () else k
                for k, v in sorted(value.items())
                if v is not None
            )
        return str(value)

    def _transform_value(self, val: Any, rule: str) -> Any:
        if isinstance(val, list):
            return [self._transform_value(x, rule) for x in val]
        if isinstance(val, dict):
            return self.rules.get(rule, _kwid)(**self._transform_node(val))
        return val

    def _transform_node(self, node: Any) -> Any:
        if isinstance(node, dict):
            return {k: self._transform_value(v, k) for k, v in node.items()}
        return node

    def transform(self, node: Any, root: str = 'ROOT') -> Any:
        return self._transform_node({root: node})[root]

    def write(self, node: Any, root: str = 'ROOT') -> str:
        value = self.transform(node, root=root)
        return self.stringify(value)
