# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import shutil
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, Tuple, cast

from ...dirtask import dir_task
from ...errors import InvalidInput, MonaError
from ...files import File
from ...pluggable import Pluggable, Plugin
from ...pyhash import hash_function
from ...tasks import Task
from ..geomlib import Atom, Molecule
from .dsl import expand_dicts, parse_aims_input

__version__ = '0.1.0'
__all__ = ['Aims', 'SpeciesDefaults']


class AimsPlugin(Plugin['Aims']):
    def process(self, kwargs: Dict[str, Any]) -> None:
        pass

    def _func_hash(self) -> str:
        return hash_function(self.process)


class Aims(Pluggable):
    """A task factory that creates FHI-aims directory tasks."""

    def __init__(self) -> None:
        Pluggable.__init__(self)
        for factory in default_plugins:
            factory()(self)

    def __call__(self, *, label: str = None, **kwargs: Any) -> Task[Dict[str, File]]:
        """Create an FHI-aims.

        :param kwargs: processed by individual plugins
        """
        self.run_plugins('process', kwargs)
        script = File.from_str('aims.sh', kwargs.pop('script'))
        inputs = [File.from_str(name, cont) for name, cont in kwargs.pop('inputs')]
        if kwargs:
            raise InvalidInput(f'Unknown Aims kwargs: {list(kwargs.keys())}')
        return dir_task(script, inputs, label=label)

    def _func_hash(self) -> str:
        return ','.join(
            [
                hash_function(Aims.__call__),
                *(cast(AimsPlugin, p)._func_hash() for p in self._get_plugins()),
            ]
        )


class SpeciesDir(AimsPlugin):
    def __init__(self) -> None:
        self._speciesdirs: Dict[Tuple[str, str], Path] = {}

    def process(self, kwargs: Dict[str, Any]) -> None:
        sp_def_key = aims, sp_def = kwargs['aims'], kwargs.pop('species_defaults')
        speciesdir = self._speciesdirs.get(sp_def_key)
        if not speciesdir:
            pathname = shutil.which(aims)
            if not pathname:
                pathname = shutil.which('aims-master')
            if not pathname:
                raise MonaError(f'Aims "{aims}" not found')
            path = Path(pathname)
            speciesdir = path.parents[1] / 'aimsfiles/species_defaults' / sp_def
            self._speciesdirs[sp_def_key] = speciesdir  # type: ignore
        kwargs['speciesdir'] = speciesdir


class Atoms(AimsPlugin):
    def process(self, kwargs: Dict[str, Any]) -> None:
        if 'atoms' in kwargs:
            kwargs['geom'] = Molecule([Atom(*args) for args in kwargs.pop('atoms')])


class SpeciesDefaults(AimsPlugin):
    """Aims plugin that handles adding species defaults to control.in."""

    def __init__(self, mod: Callable[..., Any] = None) -> None:
        self._species_defs: Dict[Tuple[Path, str], Dict[str, Any]] = {}
        self._mod = mod

    def process(self, kwargs: Dict[str, Any]) -> None:  # noqa: D102
        speciesdir = kwargs.pop('speciesdir')
        all_species = {(a.number, a.species) for a in kwargs['geom'].centers}
        species_defs = []
        for Z, species in sorted(all_species):
            if (speciesdir, species) not in self._species_defs:
                species_def = parse_aims_input(
                    (speciesdir / f'{Z:02d}_{species}_default').read_text()
                )['species'][0]
                self._species_defs[speciesdir, species] = species_def
            else:
                species_def = self._species_defs[speciesdir, species]
            species_defs.append(species_def)
        if self._mod:
            species_defs = deepcopy(species_defs)
            self._mod(species_defs, kwargs)
        kwargs['species_defs'] = species_defs

    def _func_hash(self) -> str:
        if not self._mod:
            return super()._func_hash()
        funcs = self.process, self._mod
        return ','.join(hash_function(f) for f in funcs)  # type: ignore


class Control(AimsPlugin):
    def process(self, kwargs: Dict[str, Any]) -> None:
        species_tags = []
        for spec in kwargs.pop('species_defs'):
            spec = OrderedDict(spec)
            while spec:
                tag, value = spec.popitem(last=False)
                if tag == 'angular_grids':
                    species_tags.append((tag, value))
                    for grid in spec.pop('grids'):
                        species_tags.extend(grid.items())
                elif tag == 'basis':
                    for basis in value:
                        species_tags.extend(basis.items())
                else:
                    species_tags.append((tag, value))
        species_tags = [(t, expand_dicts(v)) for t, v in species_tags]
        tags = [*kwargs.pop('tags').items(), *species_tags]
        lines = []
        for tag, value in tags:
            if value is None:
                continue
            if value is ():
                lines.append(tag)
            elif isinstance(value, list):
                lines.extend(f'{tag}  {p2f(v)}' for v in value)
            else:
                lines.append(f'{tag}  {p2f(value)}')
        kwargs['control'] = '\n'.join(lines)


class Geom(AimsPlugin):
    def process(self, kwargs: Dict[str, Any]) -> None:
        kwargs['geometry'] = kwargs.pop('geom').dumps('aims')


class Core(AimsPlugin):
    def process(self, kwargs: Dict[str, Any]) -> None:
        kwargs['inputs'] = [
            ('control.in', kwargs.pop('control')),
            ('geometry.in', kwargs.pop('geometry')),
        ]


class Script(AimsPlugin):
    def process(self, kwargs: Dict[str, Any]) -> None:
        aims, check = kwargs.pop('aims'), kwargs.pop('check', True)
        lines = ['#!/bin/bash', 'set -e', f'AIMS={aims} run_aims']
        if check:
            lines.append('egrep "Have a nice day|stop_if_parser" STDOUT >/dev/null')
        kwargs['script'] = '\n'.join(lines)


default_plugins = [SpeciesDir, Atoms, SpeciesDefaults, Control, Geom, Core, Script]


def p2f(value: Any, nospace: bool = False) -> str:
    if isinstance(value, bool):
        return f'.{str(value).lower()}.'
    if isinstance(value, tuple):
        return (' ' if not nospace else ':').join(p2f(x) for x in value)
    if isinstance(value, dict):
        return ' '.join(
            f'{p2f(k)}={p2f(v, nospace=True)}' if v is not None else f'{p2f(k)}'
            for k, v in sorted(value.items())
        )
    return str(value)
