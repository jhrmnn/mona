# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from caflib.Tools import geomlib
from caflib.Tools.convert import p2f
from caflib.Configure import feature
from caflib.Logging import info, error, report
from pathlib import Path
import shutil

_reported = {}
_tags = [
    'xc', 'many_body_dispersion', 'k_grid', 'python_hook', 'sc_accuracy_eev',
    'sc_accuracy_rho', 'sc_accuracy_etot', 'sc_iter_limit', 'total_energy_method',
    'charge', 'output', 'RI_method', 'xc_pre', 'relax_unit_cell',
    'relax_geometry', 'vdw_correction_hirshfeld', 'force_occupation_basis',
    'xc_param'
]


@report
def reporter():
    for printer, msg in _reported.values():
        printer(msg)


aimses = {}
species_db = {}


@feature('aims')
def prepare_aims(task):
    aims_name = task.consume('aims_delink')
    if aims_name in aimses:
        aims, aims_path = aimses[aims_name]
    elif aims_name:
        aims_path = Path(shutil.which(aims_name)).resolve()
        aims = aims_path.name
        aimses[aims] = aims, aims_path
    else:
        aims = aims_name = task.consume('aims')
        if aims in aimses:
            aims, aims_path = aimses[aims]
        elif aims:
            aims_path = Path(shutil.which(aims))
            aimses[aims] = aims, aims_path
        else:
            error('Missing aims specification.')
    aims_command = f'AIMS={aims} run_aims'
    if aims not in _reported:
        _reported[aims] = (info, f'{aims} is {aims_path}')
    geomfile = task.consume('geomfile') or 'geometry.in'
    basis = task.consume('basis')
    if not basis:
        error('No basis was specified for aims')
    basis_root = aims_path.parents[1]/'aimsfiles/species_defaults'/basis
    subdirs = [Path(p) for p in task.consume('subdirs') or ['.']]
    if subdirs[0] != Path('.'):
        aims_command += ' >run.out 2>run.err'
    check_output = task.consume('check')
    if check_output or check_output is None:
        aims_command += ' && grep "Have a nice day" run.out >/dev/null'
    command = []
    geom = task.consume('geom')
    for subdir in subdirs:
        if geom:
            task.inputs[str(subdir/geomfile)] = geom.dumps('aims')
        else:
            try:
                geom = geomlib.loads(task.inputs[str(subdir/geomfile)], 'aims')
            except KeyError:
                error(f'No geometry file found: {task}, {task.path}')
        species = sorted(set((a.number, a.specie) for a in geom))
        chunks = []
        for filename in list(task.inputs):
            if 'control' in filename and filename.endswith('.in'):
                chunks.append('\n'.join(
                    l for l in task.inputs[filename].split('\n')
                    if not l.lstrip().startswith('#') and l.strip()
                ) + '\n')
                del task.inputs[filename]
        if 'tags' in task.attrs:
            chunk = []
            for tag, value in task.consume('tags').items():
                if value is None:
                    continue
                if value is ():
                    chunk.append(p2f(value))
                elif isinstance(value, list):
                    chunk.extend(f'{tag}  {p2f(v)}' for v in value)
                else:
                    if value == 'xc' and value.startswith('libxc'):
                        chunk.append('override_warning_libxc')
                    chunk.append(f'{tag}  {p2f(value)}')
            chunks.append('\n'.join(chunk))
        if not basis == 'none':
            for number, specie in species:
                if (basis, specie) not in species_db:
                    with (basis_root/f'{number:02d}_{specie}_default').open() as f:
                        basis_def = f.read()
                    species_db[basis, specie] = basis_def
                else:
                    basis_def = species_db[basis, specie]
                chunks.append(basis_def)
        task.inputs[str(subdir/'control.in')] = '\n\n'.join(chunks)
        if subdir == Path('.'):
            command.append(aims_command)
        else:
            command.append(f'(cd {subdir} && env { aims_command})')
    if 'command' not in task.attrs:
        task.attrs['command'] = '\n'.join(command)
