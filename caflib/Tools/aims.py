from caflib.Tools import geomlib
from caflib.Context import feature
from caflib.Utils import find_program, report, cd
from caflib.Logging import info, warn, error
from pathlib import Path
import numpy as np
import os
import shutil

_reported = {}
_tags = [
    'xc', 'many_body_dispersion', 'k_grid', 'python_hook', 'sc_accuracy_eev',
    'sc_accuracy_rho', 'sc_accuracy_etot', 'sc_iter_limit', 'total_energy_method',
    'charge', 'output'
]


def p2f(value):
    if isinstance(value, bool):
        return '.{}.'.format(str(value).lower())
    elif isinstance(value, (np.ndarray, tuple)):
        return ' '.join(p2f(x) for x in value)
    elif isinstance(value, dict):
        return ' '.join(
            '{}={}'.format(p2f(k), p2f(v)) for k, v in sorted(value.items())
        )
    else:
        return str(value)


@report
def reporter():
    for printer, msg in _reported.values():
        printer(msg)


@feature('aims')
def prepare_aims(task):
    aims = task.consume('aims_delink')
    if aims:
        aims = shutil.which(aims)
        if Path(aims).is_symlink():
            aims = os.readlink(aims)
        else:
            aims = Path(aims).name
    else:
        aims = task.consume('aims')
    aims_command = 'AIMS={} run_aims'.format(aims)
    aims_binary = find_program(aims)
    if not aims_binary:
        if aims not in _reported:
            msg = '{} does not exit'.format(aims)
            _reported[aims] = (warn, msg)
        aims_binary = find_program('aims.master')
    if not aims_binary:
        warn(msg)
        error("Don't know where to find species files")
    if aims not in _reported:
        _reported[aims] = (info, '{} is {}'.format(aims, aims_binary))
    geomfile = task.consume('geomfile') or 'geometry.in'
    basis = task.consume('basis')
    if not basis:
        error('No basis was specified for aims')
    basis_root = aims_binary.parents[1]/'aimsfiles/species_defaults'/basis
    subdirs = list(task.consume('subdirs') or ['.'])
    if subdirs[0] != '.':
        aims_command += ' >run.out 2>run.err'
    check_output = task.consume('check')
    if check_output or check_output is None:
        aims_command += ' && grep "Have a nice day" run.out >/dev/null'
    command = []
    for subdir in subdirs:
        with cd(subdir):
            if Path(geomfile).exists():
                geom = geomlib.readfile(geomfile, 'aims')
            else:
                error('No geometry file found: {}, {}'.format(task, task.path))
            species = sorted(set((a.number, a.symbol) for a in geom))
            if not Path('control.in').exists():
                error('No control file found')
            with open('control.in') as f:
                chunks = [f.read()]
            for attr in sorted(list(task.attrs)):
                if attr in _tags:
                    value = task.consume(attr)
                    if value is None:
                        continue
                    if isinstance(value, str) and value == '':
                        chunks.append('{}'.format(attr))
                    elif isinstance(value, list):
                        chunks.append('\n'.join(
                            '{}  {}'.format(attr, p2f(v)) for v in value
                        ))
                    else:
                        chunks.append('{}  {}'.format(attr, p2f(value)))
            if not basis == 'none':
                for specie in species:
                    with (basis_root/'{0[0]:02d}_{0[1]}_default'.format(specie)).open() as f:
                        chunks.append(f.read())
            if len(chunks) > 1:
                Path('control.in').unlink()
                del task.files['control.in']
                task.store_link_text('\n\n'.join(chunks), 'control.in', label=True)
            if subdir == '.':
                command.append(aims_command)
            else:
                command.append('(cd {} && env {})'.format(subdir, aims_command))
    if 'command' not in task.attrs:
        task.attrs['command'] = '\n'.join(command)
