from caflib.Tools import geomlib
from caflib.Context import feature
from caflib.Utils import find_program


@feature('aims')
def prepare_aims(task):
        aims = find_program(task.consume('aims') or 'aims')
        geom = geomlib.readfile('geometry.in', 'aims')
        species = sorted(set((a.number, a.symbol) for a in geom))
        basis = task.consume('basis')
        assert basis
        basis_root = aims.parents[1]/'aimsfiles/species_defaults'/basis
        if not basis == 'none':
            with open('control.in', 'a') as f:
                for specie in species:
                    f.write('\n')
                    with (basis_root/'{0[0]:02d}_{0[1]}_default'.format(specie)).open() as f_sp:
                        f.write(f_sp.read())
        task.attrs['command'] = 'AIMS={} run_aims'.format(aims.name)
