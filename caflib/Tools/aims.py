from caflib.Tools import geomlib
from caflib.Context import feature
from caflib.Utils import find_program, report
from caflib.Logging import info, warn, error
import subprocess
from pathlib import Path

_reported = {}


@report
def reporter():
    for printer, msg in _reported.values():
        printer(msg)


@feature('aims')
def prepare_aims(task):
    aims = task.consume('aims') or 'aims'
    try:
        aims_binary = find_program(aims)
    except subprocess.CalledProcessError:
        if aims not in _reported:
            msg = '{} does not exit'.format(aims)
            _reported[aims] = (warn, msg)
        try:
            aims_binary = find_program('aims')
        except subprocess.CalledProcessError:
            warn(msg)
            error("Don't know where to find species files")
    if aims not in _reported:
        _reported[aims] = (info, '{} is {}'.format(aims, aims_binary))
    geom = geomlib.readfile('geometry.in', 'aims')
    species = sorted(set((a.number, a.symbol) for a in geom))
    basis = task.consume('basis')
    assert basis
    basis_root = aims_binary.parents[1]/'aimsfiles/species_defaults'/basis
    if not basis == 'none':
        with open('control.in') as f:
            chunks = [f.read()]
        Path('control.in').unlink()
        del task.files['control.in']
        for specie in species:
            with (basis_root/'{0[0]:02d}_{0[1]}_default'.format(specie)).open() as f:
                chunks.append(f.read())
        task.store_link_text('\n'.join(chunks), 'control.in', label=True)
    if 'command' not in task.attrs:
        task.attrs['command'] = 'AIMS={} run_aims'.format(aims)
