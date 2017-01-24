from caflib.Tools import geomlib
from caflib.Configure import feature
from caflib.Utils import find_program
from caflib.Logging import info, warn, error, report
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
        return f'.{str(value).lower()}.'
    elif isinstance(value, (np.ndarray, tuple)):
        return ' '.join(p2f(x) for x in value)
    elif isinstance(value, dict):
        return ' '.join(
            f'{p2f(k)}={p2f(v)}' for k, v in sorted(value.items())
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
    aims_command = f'AIMS={aims} run_aims'
    aims_binary = find_program(aims)
    if not aims_binary:
        if aims not in _reported:
            msg = f'{aims} does not exit'
            _reported[aims] = (warn, msg)
        aims_binary = find_program('aims.master')
    if not aims_binary:
        warn(msg)
        error("Don't know where to find species files")
    if aims not in _reported:
        _reported[aims] = (info, f'{aims} is {aims_binary}')
    geomfile = task.consume('geomfile') or 'geometry.in'
    basis = task.consume('basis')
    if not basis:
        error('No basis was specified for aims')
    basis_root = aims_binary.parents[1]/'aimsfiles/species_defaults'/basis
    subdirs = [Path(p) for p in task.consume('subdirs') or ['.']]
    if subdirs[0] != Path('.'):
        aims_command += ' >run.out 2>run.err'
    check_output = task.consume('check')
    if check_output or check_output is None:
        aims_command += ' && grep "Have a nice day" run.out >/dev/null'
    command = []
    for subdir in subdirs:
        try:
            geom = geomlib.loads(task.inputs[str(subdir/geomfile)], 'aims')
        except KeyError:
            error(f'No geometry file found: {task}, {task.path}')
        species = sorted(set((a.number, a.symbol) for a in geom))
        if str(subdir/'control.in') not in task.inputs:
            error('No control file found')
        chunks = [task.inputs[str(subdir/'control.in')]]
        for attr in sorted(list(task.attrs)):
            if attr in _tags:
                value = task.consume(attr)
                if value is None:
                    continue
                if isinstance(value, str) and value == '':
                    chunks.append(str(attr))
                elif isinstance(value, list):
                    chunks.append('\n'.join(f'{attr}  {p2f(v)}' for v in value))
                else:
                    chunks.append(f'{attr}  {p2f(value)}')
        if not basis == 'none':
            for number, symbol in species:
                with (basis_root/f'{number:02d}_{symbol}_default').open() as f:
                    chunks.append(f.read())
        if len(chunks) > 1:
            task.inputs[str(subdir/'control.in')] = '\n\n'.join(chunks)
        if subdir == Path('.'):
            command.append(aims_command)
        else:
            command.append(f'(cd {subdir} && env { aims_command})')
    if 'command' not in task.attrs:
        task.attrs['command'] = '\n'.join(command)
