# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import re
import shutil
from pathlib import Path

from typing import Dict, Any, Tuple, Iterable

from .dirtask import dir_task, DirTaskResult
from ..tasks import Task
from ..errors import CafError
from ..pluggable import Plugin, Pluggable
from caf.Tools.convert import p2f


class AimsPlugin(Plugin):
    def process(self, task: Dict[str, Any]) -> None:
        pass


class Aims(Pluggable):
    def __init__(self, plugins: Iterable[AimsPlugin] = None) -> None:
        plugins = plugins or [factory() for factory in default_plugins]
        Pluggable.__init__(self, plugins)

    def __call__(self, *, label: str = None, **task: Any) -> Task[DirTaskResult]:
        self.run_plugins('process', task, start=None)
        inputs = {
            fname: content.encode() for fname, content in task['inputs']
        }
        script = f'#!/bin/bash\n{task["command"]}'.encode()
        return dir_task(script, inputs, label=label)


class SpeciesDir(AimsPlugin):
    name = 'species_dir'

    def __init__(self) -> None:
        self.speciedirs: Dict[Tuple[str, str], Path] = {}

    def process(self, task: Dict[str, Any]) -> None:
        basis_key = aims, basis = task['aims'], task.pop('basis')
        if basis_key in self.speciedirs:
            speciedir = self.speciedirs[basis_key]
        else:
            pathname = shutil.which(aims)
            if not pathname:
                pathname = shutil.which('aims-master')
            if not pathname:
                raise CafError(f'Aims "{aims}" not found')
            path = Path(pathname)
            speciedir = path.parents[1]/'aimsfiles/species_defaults'/basis
            self.speciedirs[basis_key] = speciedir
        task['speciedir'] = speciedir


class Tags(AimsPlugin):
    name = 'tags'

    def process(self, task: Dict[str, Any]) -> None:
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


class Command(AimsPlugin):
    name = 'command'

    def process(self, task: Dict[str, Any]) -> None:
        aims, check = task.pop('aims'), task.pop('check', True)
        command = f'AIMS={aims} run_aims'
        if check:
            command += (
                ' && egrep "Have a nice day|stop_if_parser" STDOUT >/dev/null'
            )
        task['command'] = command


class Basis(AimsPlugin):
    name = 'basis'

    def __init__(self) -> None:
        self.basis_defs: Dict[Tuple[Path, str], str] = {}

    def process(self, task: Dict[str, Any]) -> None:
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


class UncommentTier(AimsPlugin):
    name = 'uncomment_tier'

    def __init__(self) -> None:
        self._tiers_cache: Dict[Tuple[str, int], str] = {}

    def process(self, task: Dict[str, Any]) -> None:
        tier = task.pop('tier', None)
        if tier is None:
            return
        for i in range(len(task['basis'])):
            cache_key = task['basis'][i], tier
            if cache_key in self._tiers_cache:
                task['basis'][i] = self._tiers_cache[cache_key]
                continue
            buffer = ''
            tier_now = None
            for l in task['basis'][i].split('\n'):
                m = re.search(r'"(\w+) tier"', l) or re.search(r'(Further)', l)
                if m:
                    tier_now = {
                        'First': 1,
                        'Second': 2,
                        'Third': 3,
                        'Fourth': 4,
                        'Further': 5
                    }[m.group(1)]
                m = re.search(r'#?(\s*(hydro|ionic) .*)', l)
                if m:
                    l = m.group(1)
                    if not (tier_now and tier_now <= tier):
                        l = '#' + l
                if '####' in l:
                    tier_now = None
                buffer += l + '\n'
            task['basis'][i] = buffer
            self._tiers_cache[cache_key] = buffer


class Geom(AimsPlugin):
    name = 'geom'

    def process(self, task: Dict[str, Any]) -> None:
        task['geometry'] = task.pop('geom').dumps('aims')


class Core(AimsPlugin):
    name = 'core'

    def process(self, task: Dict[str, Any]) -> None:
        control = '\n\n'.join([task.pop('control'), *task.pop('basis')])
        task['inputs'] = [
            ('control.in', control),
            ('geometry.in', task.pop('geometry')),
        ]


default_plugins = [
    SpeciesDir, Tags, Command, Basis, UncommentTier, Geom, Core
]
